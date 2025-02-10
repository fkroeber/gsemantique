import geopandas as gpd
import gsemantique as gsq
import json
import os
import pandas as pd
import semantique as sq
import shutil
import warnings

from copy import deepcopy
from datetime import datetime
from shapely.geometry import box
from pathlib import Path

# load dataset catalog
ds_catalog = gsq.DatasetCatalog()
ds_catalog.load()

# set up parameter combinations to be assessed
recipes = [x.as_posix() for x in Path("recipes").rglob("*.json")]
t_intervals = [["2017-06-01", "2017-06-08"], ["2017-05-01", "2017-07-01"]]
aoi_files = [x.as_posix() for x in Path("aois").rglob("*.geojson")]
tile_handlers = ["single", "parallel"]

# set up test cases
test_cases = [
    dict(
        recipe="recipes/01_treduce.json",
        aoi_file="aois/polygon.geojson",
        t_interval=t_intervals[0],
        tile_handler="single",
        merge_mode="merged",
        out_dir=True,
        res=100,
        epsg=32634,
    ),
    dict(
        recipe="recipes/01_treduce.json",
        aoi_file="aois/polygon.geojson",
        t_interval=t_intervals[0],
        tile_handler="parallel",
        merge_mode="merged",
        out_dir=True,
        res=100,
        epsg=32634,
    ),
    dict(
        recipe="recipes/01_treduce.json",
        aoi_file="aois/polygon.geojson",
        t_interval=t_intervals[0],
        tile_handler="single",
        merge_mode="vrt_shapes",
        out_dir=True,
        res=100,
        epsg=32634,
    ),
    dict(
        recipe="recipes/01_treduce.json",
        aoi_file="aois/multipolygon.geojson",
        t_interval=t_intervals[0],
        tile_handler="single",
        merge_mode="vrt_shapes",
        out_dir=True,
        res=100,
        epsg=32754,
    ),
    dict(
        recipe="recipes/01_treduce.json",
        aoi_file="aois/multipoint.geojson",
        t_interval=t_intervals[0],
        tile_handler="single",
        merge_mode="vrt_shapes",
        out_dir=True,
        res=100,
        epsg=32634,
    ),
    dict(
        recipe="recipes/01_treduce.json",
        aoi_file="aois/polygon.geojson",
        t_interval=t_intervals[0],
        tile_handler="single",
        merge_mode="merged",
        out_dir=True,
        res=50,
        epsg=32632,
    ),
    dict(
        recipe="recipes/02_sreduce.json",
        aoi_file="aois/polygon.geojson",
        t_interval=t_intervals[0],
        tile_handler="single",
        merge_mode="merged",
        out_dir=True,
        res=100,
        epsg=32634,
    ),
    dict(
        recipe="recipes/03_tconcatenated.json",
        aoi_file="aois/polygon.geojson",
        t_interval=t_intervals[0],
        tile_handler="single",
        merge_mode="merged",
        out_dir=True,
        res=100,
        epsg=32634,
    ),
    dict(
        recipe="recipes/03_tconcatenated.json",
        aoi_file="aois/multipolygon.geojson",
        t_interval=t_intervals[1],
        tile_handler="single",
        merge_mode="merged",
        out_dir=True,
        res=100,
        epsg=32754,
    ),
    dict(
        recipe="recipes/04_sconcatenated.json",
        aoi_file="aois/polygon.geojson",
        t_interval=t_intervals[0],
        tile_handler="single",
        merge_mode="merged",
        out_dir=False,
        res=100,
        epsg=32634,
    ),
    dict(
        recipe="recipes/05_tgrouped.json",
        aoi_file="aois/multipolygon.geojson",
        t_interval=t_intervals[1],
        tile_handler="single",
        merge_mode="merged",
        out_dir=True,
        res=100,
        epsg=32754,
    ),
    dict(
        recipe="recipes/05_tgrouped.json",
        aoi_file="aois/multipoint.geojson",
        t_interval=t_intervals[1],
        tile_handler="single",
        merge_mode="merged",
        out_dir=True,
        res=100,
        epsg=32632,
    ),
    dict(
        recipe="recipes/06_sgrouped.json",
        aoi_file="aois/polygon.geojson",
        t_interval=t_intervals[0],
        tile_handler="single",
        merge_mode="merged",
        out_dir=True,
        res=100,
        epsg=32634,
    ),
    dict(
        recipe="recipes/06_sgrouped.json",
        aoi_file="aois/multipolygon.geojson",
        t_interval=t_intervals[0],
        tile_handler="single",
        merge_mode="merged",
        out_dir=True,
        res=100,
        epsg=32754,
    ),
    dict(
        recipe="recipes/07_tmulti_grouped.json",
        aoi_file="aois/polygon.geojson",
        t_interval=t_intervals[0],
        tile_handler="single",
        merge_mode="vrt_shapes",
        out_dir=True,
        res=100,
        epsg=32634,
    ),
    dict(
        recipe="recipes/08_tmulti_double_strat.json",
        aoi_file="aois/polygon.geojson",
        t_interval=t_intervals[0],
        tile_handler="single",
        merge_mode="vrt_shapes",
        out_dir=True,
        res=100,
        epsg=32634,
    ),
    dict(
        recipe="recipes/09_udf.json",
        aoi_file="aois/polygon.geojson",
        t_interval=t_intervals[0],
        tile_handler="single",
        merge_mode="merged",
        out_dir=True,
        res=100,
        epsg=32634,
    ),
]


class Tester:
    def __init__(
        self,
        recipe,
        aoi_file,
        t_interval=["2017-06-01", "2017-07-01"],
        tile_handler="single",
        merge_mode="merged",
        out_dir=False,
        res=100,
        epsg=3857,
    ):
        self.recipe = recipe
        self.t_interval = t_interval
        self.aoi = gpd.read_file(aoi_file).to_crs(4326)
        self.tile_handler = tile_handler
        self.merge_mode = merge_mode
        self.out_dir = out_dir
        self.res = res
        self.epsg = epsg
        # execute workflow
        self._create_context()
        self._run_model()
        # check that results have been produced without any errors
        assert len(self.th.tile_results) > 0

    def _create_context(self):
        # define custom verbs
        custom_dict = {"change_dtype": gsq.change_dtype, "update_na": gsq.update_na}

        # load mapping
        with open("mapping.json", "r") as file:
            rules = json.load(file)
        mapping = sq.mapping.Semantique(rules)

        # load recipe
        with open(self.recipe, "r") as file:
            recipe = json.load(file)
        recipe = sq.QueryRecipe(recipe)

        # find data
        fdr = gsq.Finder(ds_catalog, self.t_interval[0], self.t_interval[1], self.aoi)
        fdr.search_auto(recipe, mapping, custom_verbs=custom_dict)

        # init datacube
        with open(gsq.LAYOUT_PATH, "r") as file:
            dc = sq.datacube.STACCube(
                json.load(file),
                src=fdr.item_coll,
                group_by_solar_day=True,
                dask_params=None,
            )

        # define spatio-temporal context vars
        time = sq.TemporalExtent(
            pd.Timestamp(fdr.params_search["t_start"]),
            pd.Timestamp(fdr.params_search["t_end"]),
        )
        space = sq.SpatialExtent(self.aoi)

        # compose to context dict
        context = {
            "recipe": recipe,
            "datacube": dc,
            "mapping": mapping,
            "space": space,
            "time": time,
            "crs": self.epsg,
            "tz": "UTC",
            "spatial_resolution": [-self.res, self.res],
            "caching": True,
            "track_types": False,
        }
        context = deepcopy(context)
        context["custom_verbs"] = custom_dict
        self.context = context

    def _run_model(self):
        if self.out_dir:
            # define output directory
            out_dir = os.path.splitext(
                os.path.split(os.path.normpath(self.recipe))[-1]
            )[0]
            out_dir = f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{out_dir}"
            out_dir = f"results/{out_dir}"
            if os.path.exists(out_dir):
                shutil.rmtree(out_dir)
        else:
            out_dir = None

        # just for debugging purposes
        if self.tile_handler == "None":
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", UserWarning)
                context = self.context
                context["preview"] = False
                context["caching"] = False
                self.response = self.recipe.execute(**context)

        if self.tile_handler == "single":
            self.th = gsq.TileHandler(
                chunksize_s=256,
                chunksize_t="2W",
                merge_mode=self.merge_mode,
                out_dir=out_dir,
                reauth=False,
                verbose=True,
                **self.context,
            )
            self.th.execute()

        elif self.tile_handler == "parallel":
            self.th = gsq.TileHandlerParallel(
                chunksize_s=128,
                merge_mode=self.merge_mode,
                out_dir=out_dir,
                reauth=False,
                verbose=True,
                n_procs=min(2, os.cpu_count()),
                **self.context,
            )
            self.th.execute()


if __name__ == "__main__":
    for i, test_case in enumerate(test_cases):
        print(f"\nRunning test no. {i}")
        tester = Tester(**test_case)
