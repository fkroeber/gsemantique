import numpy as np
import pandas as pd
import pystac
import planetary_computer as pc
from pystac_client import Client
from .datasets import DatasetCatalog

ds_catalog = DatasetCatalog()


class Finder:
    """
    Searches a given catalog to output the item's STAC metadata
    (-> which can then be forwarded to STACCube)
    """

    def __init__(self, t_start, t_end, aoi, provider, collection, layer_key):
        self.params_in = {
            "t_start": t_start,
            "t_end": t_end,
            "aoi": aoi,
            "provider": provider,
            "collection": collection,
            "layer_key": layer_key,
        }
        self.params_search = {}

    def retrieve_params(self):
        """
        Retrieves the data search parameters based on the specified inputs
        """
        # get specific collection & make sure that it is filtered
        collection = ds_catalog.filter(
            collection=self.params_in["collection"],
            provider=self.params_in["provider"],
        )
        assert len(collection) == 1
        collection = collection.iloc[0]

        # retrieve data parameters
        self.params_search["provider"] = collection["provider"]
        self.params_search["catalog"] = collection["endpoint"]
        self.params_search["collection"] = collection["collection"]
        self.params_search["temp"] = collection["temporality"]
        self.params_search["lfile"] = collection["layout_file"]
        self.params_search["lkeys"] = collection["layout_keys"]
        self.params_search["aoi"] = self.params_in["aoi"]

        # ensure that key is in layout file
        assert self.params_in["layer_key"] in self.params_search["lkeys"]

        # retrieve time range for query
        if self.params_search["temp"]:
            self.params_search["t_start"] = np.datetime64(self.params_in["t_start"])
            self.params_search["t_end"] = np.datetime64(self.params_in["t_end"])
        else:
            self.params_search["t_start"] = np.datetime64("1970-01-01")
            self.params_search["t_end"] = np.datetime64("today")

    def retrieve_metadata(self):
        """
        Performs the data search based on the retrieved params
        """
        # init catalog
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
            limit=100,
            intersects=self.params_search["aoi"],
        )
        item_coll = query.item_collection()

        # filter search results
        self.item_coll = self._postprocess_search(item_coll)

        # print response
        item_dicts = [item.to_dict() for item in self.item_coll]
        print(f"Found: {len(item_dicts):d} datasets")

    def postprocess_metadata(self):
        """
        Postprocesses the metadata before passing it back
        """
        # set collection items datetime (if not provided)
        for item in self.item_coll:
            pystac.Item.from_dict(
                item.to_dict()
            )  # necessary to create datetime if not set yet
            if not item.properties["datetime"]:
                start_time = pd.Timestamp(item.properties["start_datetime"])
                end_time = pd.Timestamp(item.properties["end_datetime"])
                mean_time = start_time + (end_time - start_time) / 2
                item.set_datetime(mean_time)

    def _postprocess_search(self, item_coll):
        """
        Method allowing to subset/modify the search results of a STAC search,
        only needed if STAC organisation of data is not standard-conform,
        e.g. if bands are not organised as assets but as items
        """
        if (self.params_search["provider"] == "ASF") & (
            self.params_search["collection"] == "sentinel-1-global-coherence"
        ):
            suffix = self.params_in["layer_key"][-1].rsplit("_", 2)[1:]
            if len(suffix) == 2:
                var, pol = suffix
                item_coll = [
                    x
                    for x in item_coll
                    if x.properties["sar:product_type"] == var.upper()
                ]
                item_coll = [
                    x
                    for x in item_coll
                    if x.properties["sar:polarizations"] == [pol.upper()]
                ]
            else:
                var = suffix[0]
                item_coll = [
                    x for x in item_coll if x.properties["sar:product_type"] == var
                ]
        return item_coll
