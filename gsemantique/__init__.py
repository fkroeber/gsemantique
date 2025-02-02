import os
import gsemantique

# import sub-modules & functions
from gsemantique.data.download import Downloader
from gsemantique.data.datasets import Dataset, DatasetCatalog
from gsemantique.data.search import Finder

from gsemantique.process.scaling import TileHandler, TileHandlerParallel
from gsemantique.process.utils import update_na, change_dtype

# get default layout path for pre-defined data catalog
package_dir = os.path.split(gsemantique.__file__)[0]
LAYOUT_PATH = os.path.join(package_dir, "data", "layout.json")
