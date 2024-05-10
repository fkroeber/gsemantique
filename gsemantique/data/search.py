import json
import logging
import numpy as np
import os
import pandas as pd
import planetary_computer as pc
import pystac
import xarray as xr
from pystac_client import Client
from semantique.processor.core import FakeProcessor
from .datasets import Dataset

FILE_PATH = os.path.dirname(os.path.abspath(__file__))
logger = logging.getLogger(__name__)


class Finder:
    """
    Searches a given catalog to output the item's STAC metadata
    This can then be forwarded to STACCube.
    """

    def __init__(
        self,
        ds_catalog,
        t_start,
        t_end,
        aoi,
        layout_file=os.path.join(FILE_PATH, "../data/layout.json"),
    ):
        """
        Initializes the Finder object with the specified inputs

        Args:
            ds_catalog (DatasetCatalog): Dataset catalog containing the data sets to be searched
            t_start (str): Start time of the search
            t_end (str): End time of the search
            aoi (shapely.geometry): Area of interest for the search.
            layout_file (str): Path to the datacube layout file
        """
        self.layout_file = layout_file
        self.ds_catalog = ds_catalog
        self.t_start = t_start
        self.t_end = t_end
        self.aoi = aoi
        self.params_search = {}

    def search_auto(self, recipe, mapping, datacube):
        # fake run to resolve data references
        fp = FakeProcessor(
            recipe=recipe,
            datacube=datacube,
            mapping=mapping,
            extent=xr.DataArray(),
        )
        _ = fp.optimize().execute()
        layer_keys = list(set(fp.cache.seq))
        # log info
        logger.info("The recipe references the following data layers:")
        for key in layer_keys:
            logger.info(key)
        # run manual search for each data layer
        item_colls = []
        for layer_key in layer_keys:
            self.search_man(layer_key)
            item_colls.append(list(self.item_coll))
        # compile results
        self.item_coll = [x for sl in item_colls for x in sl]
        self.item_coll = pystac.ItemCollection(self.item_coll)
        self.item_coll = self._merge_assets_per_item(self.item_coll)

    def search_man(self, layer_key):
        self._retrieve_params(layer_key)
        self._retrieve_metadata(layer_key)
        self._postprocess_search(layer_key)
        logger.info(f"Found {len(self.item_coll):d} datasets")

    def _retrieve_params(self, layer_key):
        """
        Retrieves the data search parameters based on the specified inputs
        """
        # get specific collection
        ds_table = self.ds_catalog.parse_as_table(keys=None)
        keep = ds_table["layout_keys"].apply(lambda x: layer_key in x)
        ds_entry = ds_table[keep].iloc[0]

        # retrieve data parameters
        self.params_search["provider"] = ds_entry["provider"]
        self.params_search["catalog"] = ds_entry["endpoint"]
        self.params_search["collection"] = ds_entry["collection"]
        self.params_search["temp"] = ds_entry["temporality"]
        self.params_search["lkeys"] = ds_entry["layout_keys"]
        self.params_search["aoi"] = self.aoi

        # retrieve time range for query
        if self.params_search["temp"]:
            self.params_search["t_start"] = np.datetime64(self.t_start)
            self.params_search["t_end"] = np.datetime64(self.t_end)
        else:
            self.params_search["t_start"] = np.datetime64("1970-01-01")
            self.params_search["t_end"] = np.datetime64("today")

    def _retrieve_metadata(self, layer_key):
        """
        Performs the data search based on the retrieved params
        """
        # init search client
        if self.params_search["provider"] == "Planet":
            catalog = Client.open(
                self.params_search["catalog"], modifier=pc.sign_inplace
            )
        else:
            catalog = Client.open(self.params_search["catalog"])

        # make search
        query = catalog.search(
            collections=self.params_search["collection"],
            datetime=[
                np.datetime_as_string(self.params_search["t_start"], timezone="UTC"),
                np.datetime_as_string(self.params_search["t_end"], timezone="UTC"),
            ],
            intersects=self.params_search["aoi"],
        )
        self.item_coll = query.item_collection()

    def _postprocess_search(self, layer_key):
        """
        Method allowing to subset/modify the search results of a STAC search,
        only needed if STAC organisation of data is not standard-conform,
        e.g. if bands are not organised as assets but as items
        """
        # collection-specific postprocessing
        if (self.params_search["provider"] == "ASF") & (
            self.params_search["collection"] == "sentinel-1-global-coherence"
        ):
            suffix = layer_key[-1].rsplit("_", 2)[1:]
            if len(suffix) == 2:
                var, pol = suffix
                item_coll = [
                    x
                    for x in self.item_coll
                    if x.properties["sar:product_type"] == var.upper()
                    and x.properties["sar:polarizations"] == [pol.upper()]
                ]
            else:
                var = suffix[0]
                item_coll = [
                    x for x in self.item_coll if x.properties["sar:product_type"] == var
                ]
            self.item_coll = item_coll

        # collection-indifferent postprocessing
        with open(self.layout_file, "r") as f:
            layout_json = json.load(f)
            parsed_layout = Dataset._parse_layout(layout_json)
        for item in self.item_coll:
            # write layout key to assets extra field
            asset_name = Dataset._lookup(parsed_layout, *layer_key)["name"]
            if asset_name in item.assets:
                asset_dict = item.assets[asset_name].to_dict()
                asset_dict["semantique:key"] = layer_key
                modified_asset = pystac.Asset.from_dict(asset_dict)
                item.assets[asset_name] = modified_asset
            # subset item's assets to searched asset
            new_assets = {asset_name: item.assets[asset_name]}
            item.assets = new_assets
            # set collection items datetimes
            if not item.properties["datetime"]:
                start_time = pd.Timestamp(item.properties["start_datetime"])
                end_time = pd.Timestamp(item.properties["end_datetime"])
                mean_time = start_time + (end_time - start_time) / 2
                item.set_datetime(mean_time)

    def _merge_assets_per_item(self, item_collection):
        """
        Merges items in an ItemCollection that have the same ID and belong to the same collection
        by combining their assets.

        Parameters:
        - item_collection (pystac.ItemCollection): The collection of items to process.

        Returns:
        - pystac.ItemCollection: A new item collection with merged items.
        """
        merged_items = {}
        for item in item_collection.items:
            # combine item ID and collection ID to form a unique key
            unique_key = f"{item.get_collection().id}::{item.id}"
            if unique_key in merged_items:
                existing_item = merged_items[unique_key]
                for asset_key, asset in item.assets.items():
                    if asset_key not in existing_item.assets:
                        existing_item.add_asset(asset_key, asset)
            else:
                merged_items[unique_key] = item.clone()
        # create a new item collection from the merged items
        new_items = list(merged_items.values())
        return pystac.ItemCollection(new_items)
