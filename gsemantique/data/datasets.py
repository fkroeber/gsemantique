import copy
import json
import numpy as np
import os
import pandas as pd
import planetary_computer as pc
import pickle
from pystac_client import Client

FILE_PATH = os.path.dirname(os.path.abspath(__file__))


class Dataset:
    """A STAC-compliant dataset/collection including provider information."""

    def __init__(
        self,
        provider,
        endpoint,
        collection,
        temporality,
        temporal_extent=None,
        spatial_extent=None,
        category=None,
        src=None,
        info=None,
        copyright=None,
    ):
        """
        params:
            provider (str): arbitrary abbreviation of dataset provider
            endpoint (str): STAC catalog endpoint of the dataset
            collection (str): STAC collection name of the dataset
            temporality (str): frequency of data acquistions
            temporal_extent (lst): temporal coverage, can be auto-inferred
            spatial_extent (lst): spatial coverage, can be auto-inferred
            category (str): arbitrary categorisation of the dataset
            src (str): source of dataset
            info (str): general information on the dataset
            copyright (str): license information

        Note: temporality should be given in offset units as defined here
        (https://pandas.pydata.org/docs/user_guide/timeseries.html#dateoffset-objects)
        """
        self.provider = provider
        self.endpoint = endpoint
        self.collection = collection
        self.category = category
        self.temporality = temporality
        self.temporal_extent = temporal_extent
        self.spatial_extent = spatial_extent
        self.src = src
        self.info = info
        self.copyright = copyright
        # get extents if not provided
        self._auto_infer_extents()
        # init empty attributes for layout
        self.layout_keys = []
        self.layout_bands = {}

    def add_layout_info(
        self,
        keys,
        file=os.path.join(FILE_PATH, "layout.json"),
    ):
        """
        Adds dataset information from layout.json,
        i.e. the band keys & their attributes
        """
        self.layout_keys = keys
        with open(file, "r") as f:
            layout_json = json.load(f)
            parsed_layout = Dataset._parse_layout(layout_json)
            for k in self.layout_keys:
                self.layout_bands[k[-1]] = Dataset._lookup(parsed_layout, *k)

    def _auto_infer_extents(self):
        """
        Automatically infers the spatial and temporal extents of the dataset
        from the STAC collection if they are not provided.
        """
        if self.spatial_extent is None or self.temporal_extent is None:
            try:
                # get collection metadata
                if self.provider == "Planetary":
                    catalog = Client.open(self.endpoint, modifier=pc.sign_inplace)
                else:
                    catalog = Client.open(self.endpoint)
                collection = catalog.get_child(self.collection)
                # parse spatial and temporal extents
                if self.spatial_extent is None:
                    spatial_extent = collection.extent.spatial.bboxes
                    self.spatial_extent = np.round(spatial_extent).tolist()[0]
                if self.temporal_extent is None:
                    self.temporal_extent = collection.extent.temporal.intervals[0]
            except Exception as e:
                print(f"Failed to auto-infer extents: {e}")

    @staticmethod
    def _parse_layout(obj):
        """
        Function to recursively parse and metadata objects from layout.json
        and to make them autocomplete friendly
        """

        def _parse(current_obj, ref_path):
            if "type" in current_obj and "values" in current_obj:
                current_obj["reference"] = copy.deepcopy(ref_path)
                if isinstance(current_obj["values"], list):
                    current_obj["labels"] = {
                        item["label"]: item["id"] for item in current_obj["values"]
                    }
                    current_obj["descriptions"] = {
                        item["description"]: item["id"]
                        for item in current_obj["values"]
                    }
                return
            # If not a "layer", traverse deeper into the object.
            for key, value in current_obj.items():
                if isinstance(value, dict):
                    new_ref_path = ref_path + [key]
                    _parse(value, new_ref_path)

        # Start parsing from the root object.
        for key, value in obj.items():
            if isinstance(value, dict):
                _parse(value, [key])
        return obj

    @staticmethod
    def _lookup(obj, *reference):
        """Lookup the metadata of a referenced data layer.

        Parameters
        ----------
            *reference:
            The index of the data layer in the layout of the EO data cube.

        Raises
        -------
            :obj:`exceptions.UnknownLayerError`
            If the referenced data layer does not have a metadata object in the
            layout of the EO data cube.

        """
        for key in reference:
            obj = obj[key]
        return obj


class DatasetCatalog:
    """
    Container to hold all datasets.
    Call `print(<DatasetCatalog>)` to see the datasets content.
    """

    def __str__(self):
        if self.datasets:
            catalog_df = self.parse_as_table(keys=None)
            n_prov = len(catalog_df["provider"].unique())
            n_ds = len(self.datasets)
            n_lyr = sum(catalog_df["n_bands"])
            catalog_info = "DatasetCatalog containing\n"
            prov_info = f"- {n_prov} providers (catalogs)\n"
            ds_info = f"- {n_ds} datasets (collections)\n"
            lyr_info = f"- {n_lyr} layers (assets)\n"
            header_info = f"{catalog_info}{prov_info}{ds_info}{lyr_info}"
            wrapped_text = "\n".join([line for line in header_info.split("\n") if line])
            dataset_info = f"\n\nDatasets:\n{self.parse_as_table()}"
            return wrapped_text + dataset_info
        else:
            return "DatasetCatalog empty -> call .load() or .add() to populate the catalogue."

    def __init__(self, cache_path=os.path.join(FILE_PATH, "data_cache.pkl")):
        self.cache_path = cache_path
        self.datasets = []

    def add(self, dataset):
        """Adds a new dataset as an instance `Dataset` of to the catalog."""
        self.datasets.append(dataset)

    def filter(self, **criteria):
        """Filters catalog according to dataset attributes."""
        df = self.parse_as_table(keys=None)
        for attr, value in criteria.items():
            if value is not None:
                if isinstance(value, list):
                    df = df[df[attr].isin(value)]
                else:
                    df = df[df[attr] == value]
            else:
                df = df[df[attr].isna()]
        return df

    def load(self, from_cache=True, cache_path=None):
        """
        Loads the current data catalog either from cache or by
        evaluating the available datasets as given by the list of
        default data sets (see `_load_defaults`).
        """
        if not cache_path:
            cache_path = self.cache_path
        if from_cache:
            if os.path.exists(cache_path):
                with open(cache_path, "rb") as f:
                    self.datasets = pickle.load(f)
            else:
                raise ValueError(f"{cache_path} does not exist.")
        else:
            self._load_defaults()

    def parse_as_table(
        self,
        keys=["provider", "collection", "category", "temporality"],
    ):
        """
        Creates and returns a DataFrame containing all datasets,
        with optional filtering of keys.
        """
        data = []
        for dataset in self.datasets:
            # compile dataset attributes
            dataset_attributes = {
                attr: getattr(dataset, attr)
                for attr in dir(dataset)
                if not attr.startswith("__") and not callable(getattr(dataset, attr))
            }
            dataset_attributes.update({"n_bands": len(dataset.layout_keys)})
            # filter dataset attributes
            if keys:
                dataset_attributes = {
                    key: dataset_attributes.get(key, None) for key in keys
                }
            data.append(dataset_attributes)
        df = pd.DataFrame(data)
        return df

    def parse_as_dict(self, keys=None):
        """Creates and returns a dictionary representation of the catalog with optional filtering of keys."""
        catalog_dict = {}
        for dataset in self.datasets:
            # Dynamically gather dataset attributes
            dataset_attributes = {
                attr: getattr(dataset, attr)
                for attr in dir(dataset)
                if not attr.startswith("__") and not callable(getattr(dataset, attr))
            }
            # Filter the attributes to include only those keys
            if keys:
                dataset_attributes = {
                    key: dataset_attributes[key]
                    for key in keys
                    if key in dataset_attributes
                }
            # Use provider name and collection name as key
            provider = dataset_attributes.pop("provider")
            collection_name = dataset_attributes.pop("collection")
            if provider not in catalog_dict:
                catalog_dict[provider] = {
                    "endpoint": dataset.endpoint,
                    "datasets": {},
                }
            catalog_dict[provider]["datasets"][collection_name] = dataset_attributes

        return catalog_dict

    def save(self, cache_path=None):
        """
        Saves the current data catalog as a pickled object.
        """
        if not cache_path:
            cache_path = self.cache_path
        with open(cache_path, "wb") as f:
            pickle.dump(self.datasets, f)

    def _load_defaults(self):
        ds = Dataset(
            provider="Planetary",
            endpoint="https://planetarycomputer.microsoft.com/api/stac/v1",
            collection="sentinel-1-rtc",
            category="SAR",
            temporality="s",
            temporal_extent=None,
            spatial_extent=None,
            src="https://planetarycomputer.microsoft.com/dataset/sentinel-1-rtc",
            info="Sentinel-1 represent radar imaging (SAR) satellites launched in 2014 and 2016 with a 6 days revisit cycle. It's C-Band radar has the ability to penetrate clouds. The Sentinel-1 RTC data in this collection is a radiometrically terrain corrected product (gamma naught values) derived from the Ground Range Detected (GRD) Level-1 products produced by the European Space Agency. It accounts for terrain variations that affect both the position of a given point on the Earth's surface and the brightness of the radar return, as expressed in radar geometry. Without treatment, the hill-slope modulations of the radiometry threaten to overwhelm weaker thematic land cover-induced backscatter differences. Additionally, comparison of backscatter from multiple satellites, modes, or tracks loses meaning.",
            copyright="CC BY 4.0",
        )
        ds.add_layout_info(
            [
                ("Planetary", "reflectance", "s1_amp_vv"),
                ("Planetary", "reflectance", "s1_amp_vh"),
                ("Planetary", "reflectance", "s1_amp_hv"),
                ("Planetary", "reflectance", "s1_amp_hh"),
            ],
        )
        self.add(ds)

        ds = Dataset(
            provider="Planetary",
            endpoint="https://planetarycomputer.microsoft.com/api/stac/v1",
            collection="sentinel-2-l2a",
            category="multispectral",
            temporality="s",
            temporal_extent=None,
            spatial_extent=None,
            src="https://planetarycomputer.microsoft.com/dataset/sentinel-2-l2a",
            info="The Sentinel-2 program provides global imagery in thirteen spectral bands at 10m-60m resolution and a revisit time of approximately five days. This dataset represents the global Sentinel-2 archive, from 2016 to the present, processed to L2A (bottom-of-atmosphere) using Sen2Cor and converted to cloud-optimized GeoTIFF format.",
            copyright="Copernicus Sentinel Data Terms",
        )
        ds.add_layout_info(
            [
                ("Planetary", "reflectance", "s2_band01"),
                ("Planetary", "reflectance", "s2_band02"),
                ("Planetary", "reflectance", "s2_band03"),
                ("Planetary", "reflectance", "s2_band04"),
                ("Planetary", "reflectance", "s2_band05"),
                ("Planetary", "reflectance", "s2_band06"),
                ("Planetary", "reflectance", "s2_band07"),
                ("Planetary", "reflectance", "s2_band08"),
                ("Planetary", "reflectance", "s2_band08A"),
                ("Planetary", "reflectance", "s2_band09"),
                ("Planetary", "reflectance", "s2_band11"),
                ("Planetary", "reflectance", "s2_band12"),
                ("Planetary", "classification", "scl"),
            ],
        )
        self.add(ds)

        ds = Dataset(
            provider="Planetary",
            endpoint="https://planetarycomputer.microsoft.com/api/stac/v1",
            collection="landsat-c2-l2",
            category="multispectral",
            temporality="s",
            temporal_extent=None,
            spatial_extent=None,
            src="https://planetarycomputer.microsoft.com/dataset/landsat-c2-l2",
            info="Landsat Collection 2 Level-2 Science Products, consisting of atmospherically corrected surface reflectance and surface temperature image data. Collection 2 Level-2 Science Products are available from August 22, 1982 to present.This dataset represents the global archive acquired by the Thematic Mapper onboard Landsat 4 and 5, the Enhanced Thematic Mapper onboard Landsat 7, and the Operatational Land Imager and Thermal Infrared Sensor onboard Landsat 8 and 9.",
            copyright="Public Domain (https://www.usgs.gov/emergency-operations-portal/data-policy)",
        )
        ds.add_layout_info(
            [
                ("Planetary", "reflectance", "lndst_coastal"),
                ("Planetary", "reflectance", "lndst_blue"),
                ("Planetary", "reflectance", "lndst_green"),
                ("Planetary", "reflectance", "lndst_red"),
                ("Planetary", "reflectance", "lndst_nir08"),
                ("Planetary", "reflectance", "lndst_swir16"),
                ("Planetary", "reflectance", "lndst_swir22"),
                ("Planetary", "reflectance", "lndst_lwir109"),
                ("Planetary", "reflectance", "lndst_lwir114"),
                ("Planetary", "reflectance", "lndst_qa"),
            ],
        )
        self.add(ds)

        ds = Dataset(
            provider="Planetary",
            endpoint="https://planetarycomputer.microsoft.com/api/stac/v1",
            collection="esa-worldcover",
            category="landcover",
            temporality="Y",
            temporal_extent=None,
            spatial_extent=None,
            src="https://planetarycomputer.microsoft.com/dataset/esa-worldcover",
            info="The European Space Agency (ESA) WorldCover product provides global land cover maps for the years 2020 and 2021 at 10 meter resolution based on the combination of Sentinel-1 radar data and Sentinel-2 imagery. The discrete classification maps provide 11 classes defined using the Land Cover Classification System (LCCS) developed by the United Nations (UN) Food and Agriculture Organization (FAO). The map images are stored in cloud-optimized GeoTIFF format. The WorldCover product is developed by a consortium of European service providers and research organizations. VITO (Belgium) is the prime contractor of the WorldCover consortium.",
            copyright="Creative Commons Attribution 4.0 International License",
        )
        ds.add_layout_info(
            [
                ("Planetary", "classification", "esa_lc"),
            ],
        )
        self.add(ds)

        ds = Dataset(
            provider="Planetary",
            endpoint="https://planetarycomputer.microsoft.com/api/stac/v1",
            collection="io-lulc-annual-v02",
            category="landcover",
            temporality="Y",
            temporal_extent=None,
            spatial_extent=None,
            src="https://planetarycomputer.microsoft.com/dataset/io-lulc-annual-v02",
            info="Time series of annual global maps of land use and land cover (LULC). It currently has data from 2017-2023. The maps are derived from ESA Sentinel-2 imagery at 10m resolution. Each map is a composite of LULC predictions for 9 classes throughout the year in order to generate a representative snapshot of each year. This dataset is produced by Impact Observatory, Microsoft, and Esri. This dataset was generated by Impact Observatory, which used billions of human-labeled pixels (curated by the National Geographic Society) to train a deep learning model for land classification.",
            copyright="Creative Commons BY-4.0",
        )
        ds.add_layout_info(
            [
                ("Planetary", "classification", "impact_lc"),
            ],
        )
        self.add(ds)

        ds = Dataset(
            provider="Planetary",
            endpoint="https://planetarycomputer.microsoft.com/api/stac/v1",
            collection="nasadem",
            category="DEM",
            temporality=None,
            temporal_extent=None,
            spatial_extent=None,
            src="https://planetarycomputer.microsoft.com/dataset/nasadem",
            info="NASADEM provides global topographic data at 1 arc-second (~30m) horizontal resolution, derived primarily from data captured via the Shuttle Radar Topography Mission (SRTM).",
            copyright="Public Domain (https://lpdaac.usgs.gov/data/data-citation-and-policies/)",
        )
        ds.add_layout_info(
            [
                ("Planetary", "topography", "dem"),
            ],
        )
        self.add(ds)

        ds = Dataset(
            provider="Planetary",
            endpoint="https://planetarycomputer.microsoft.com/api/stac/v1",
            collection="cop-dem-glo-30",
            category="DSM",
            temporality=None,
            temporal_extent=None,
            spatial_extent=None,
            src="https://planetarycomputer.microsoft.com/dataset/cop-dem-glo-30",
            info="The Copernicus DEM is a digital surface model (DSM), which represents the surface of the Earth including buildings, infrastructure, and vegetation. This DSM is based on radar satellite data acquired during the TanDEM-X Mission, which was funded by a public-private partnership between the German Aerospace Centre (DLR) and Airbus Defence and Space. Copernicus DEM is available at both 30-meter and 90-meter resolution; this dataset has a horizontal resolution of approximately 30 meters.",
            copyright=None,
        )
        ds.add_layout_info(
            [
                ("Planetary", "topography", "dsm"),
            ],
        )
        self.add(ds)

        ds = Dataset(
            provider="Planetary",
            endpoint="https://planetarycomputer.microsoft.com/api/stac/v1",
            collection="modis-64A1-061",
            category="fire detection",
            temporality="M",
            temporal_extent=None,
            spatial_extent=None,
            src="https://planetarycomputer.microsoft.com/dataset/modis-64A1-061",
            info="The Terra and Aqua combined MCD64A1 Version 6.1 Burned Area data product is a monthly, global gridded 500 m product containing per-pixel burned-area and quality information. The MCD64A1 burned-area mapping approach employs 500 m Moderate Resolution Imaging Spectroradiometer (MODIS) Surface Reflectance imagery coupled with 1 kilometer (km) MODIS active fire observations. The algorithm uses a burn sensitive Vegetation Index (VI) to create dynamic thresholds that are applied to the composite data. The VI is derived from MODIS shortwave infrared atmospherically corrected surface reflectance bands 5 and 7 with a measure of temporal texture.",
            copyright=None,
        )
        ds.add_layout_info(
            [
                ("Planetary", "burned_mapping", "m_burn_date"),
                ("Planetary", "burned_mapping", "m_burn_uncertainty"),
                ("Planetary", "burned_mapping", "m_burn_qa"),
            ],
        )
        self.add(ds)

        ds = Dataset(
            provider="Planetary",
            endpoint="https://planetarycomputer.microsoft.com/api/stac/v1",
            collection="modis-14A2-061",
            category="fire detection",
            temporality="D",
            temporal_extent=None,
            spatial_extent=None,
            src="https://planetarycomputer.microsoft.com/dataset/modis-14A2-061",
            info="The Moderate Resolution Imaging Spectroradiometer (MODIS) Thermal Anomalies and Fire 8-Day Version 6.1 data are generated at 1 kilometer (km) spatial resolution as a Level 3 product. The MOD14A2 gridded composite contains the maximum value of the individual fire pixel classes detected during the eight days of acquisition.",
            copyright=None,
        )
        ds.add_layout_info(
            [
                ("Planetary", "burned_mapping", "w_burn_qa"),
                ("Planetary", "burned_mapping", "w_burn_firemask"),
            ],
        )
        self.add(ds)

        ds = Dataset(
            provider="Planetary",
            endpoint="https://planetarycomputer.microsoft.com/api/stac/v1",
            collection="jrc-gsw",
            category="hydrogeography",
            temporality=None,
            temporal_extent=None,
            spatial_extent=None,
            src="https://planetarycomputer.microsoft.com/dataset/jrc-gsw",
            info="Global surface water products from the European Commission Joint Research Centre, based on Landsat 5, 7, and 8 imagery. Layers in this collection describe the occurrence, change, and seasonality of surface water from 1984-2020.",
            copyright="Copernicus Open Access Policy",
        )
        ds.add_layout_info(
            [
                ("Planetary", "hydrogeography", "change"),
                ("Planetary", "hydrogeography", "extent"),
                ("Planetary", "hydrogeography", "occurrence"),
                ("Planetary", "hydrogeography", "transitions"),
            ],
        )
        self.add(ds)

        ds = Dataset(
            provider="Element84",
            endpoint="https://earth-search.aws.element84.com/v1",
            collection="sentinel-2-l2a",
            category="multispectral",
            temporality="s",
            temporal_extent=None,
            spatial_extent=None,
            src="https://registry.opendata.aws/sentinel-2-l2a-cogs/",
            info="Sentinel-2 mission is a land monitoring constellation of two satellites providing high resolution optical imagery with a resolution of up to 10m. The mission provides a global coverage of the Earth's land surface every 5 days. This dataset contains all of the scenes in the original Sentinel-2 Public Dataset, except the JP2K files were converted into Cloud-Optimized GeoTIFFs (COGs). L2A data are available from April 2017 over wider Europe region and globally since December 2018.",
            copyright=None,
        )
        ds.add_layout_info(
            [
                ("Element84", "reflectance", "s2_band01"),
                ("Element84", "reflectance", "s2_band02"),
                ("Element84", "reflectance", "s2_band03"),
                ("Element84", "reflectance", "s2_band04"),
                ("Element84", "reflectance", "s2_band05"),
                ("Element84", "reflectance", "s2_band06"),
                ("Element84", "reflectance", "s2_band07"),
                ("Element84", "reflectance", "s2_band08"),
                ("Element84", "reflectance", "s2_band08A"),
                ("Element84", "reflectance", "s2_band09"),
                ("Element84", "reflectance", "s2_band11"),
                ("Element84", "reflectance", "s2_band12"),
                ("Element84", "classification", "scl"),
            ],
        )
        self.add(ds)

        ds = Dataset(
            provider="ASF",
            endpoint="https://stac.asf.alaska.edu",
            collection="sentinel-1-global-coherence",
            category="SAR",
            temporality="3M",
            temporal_extent=None,
            spatial_extent=None,
            src="https://registry.opendata.aws/ebd-sentinel-1-global-coherence-backscatter/",
            info="Global C-band Synthetic Aperture Radar (SAR) interferometric repeat-pass coherence and backscatter signatures from Sentinel-1. Timeframe: 1-Dec-2019 to 30-Nov-2020 processed in seasonal manner (December-February, March-May, June-August, September-November) with a pixel spacing of three arcseconds. Coverage comprises land masses and ice sheets from 82° Northern to 79° Southern latitudes.",
            copyright="Creative Commons Zero (CC0) 1.0 Universal License",
        )
        ds.add_layout_info(
            [
                ("ASF", "coherence", "s1_coh6_vv"),
                ("ASF", "coherence", "s1_coh6_hh"),
                ("ASF", "coherence", "s1_coh12_vv"),
                ("ASF", "coherence", "s1_coh12_hh"),
                ("ASF", "coherence", "s1_coh18_vv"),
                ("ASF", "coherence", "s1_coh18_hh"),
                ("ASF", "coherence", "s1_coh24_vv"),
                ("ASF", "coherence", "s1_coh24_hh"),
                ("ASF", "coherence", "s1_coh36_vv"),
                ("ASF", "coherence", "s1_coh36_hh"),
                ("ASF", "coherence", "s1_coh48_vv"),
                ("ASF", "coherence", "s1_coh48_hh"),
                ("ASF", "reflectance", "s1_amp_vv"),
                ("ASF", "reflectance", "s1_amp_vh"),
                ("ASF", "reflectance", "s1_amp_hv"),
                ("ASF", "reflectance", "s1_amp_hh"),
                ("ASF", "reflectance", "s1_inc"),
            ],
        )
        self.add(ds)

        ds = Dataset(
            provider="ASF",
            endpoint="https://stac.asf.alaska.edu",
            collection="glo-30-hand",
            category="hydrogeography",
            temporality=None,
            temporal_extent=None,
            spatial_extent=None,
            src="https://registry.opendata.aws/glo-30-hand/",
            info="Height Above Nearest Drainage (HAND) is a terrain model that normalizes topography to the relative heights along the drainage network and is used to describe the relative soil gravitational potentials or the local drainage potentials. Each pixel value represents the vertical distance to the nearest drainage. The HAND data provides near-worldwide land coverage at 30 meters and was produced from the 2021 release of the Copernicus GLO-30 Public DEM. The HAND data are provided as a tiled set of Cloud Optimized GeoTIFFs (COGs) with 30-meter (1 arcsecond) pixel spacing.",
            copyright="Creative Commons Attribution 4.0",
        )
        ds.add_layout_info(
            [("ASF", "hydrogeography", "hand")],
        )
        self.add(ds)
