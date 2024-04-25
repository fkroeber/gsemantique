# tbd: make paths relative
import asyncio
import numpy as np
import pystac
import os
import shutil
import stac_asset
import stac_asset.blocking
import time
from aiohttp import ClientSession
from aiohttp.client import ClientTimeout
from aiohttp_retry import RetryClient, ExponentialRetry
from datetime import datetime
from pystac.item_collection import ItemCollection
from stac_asset.http_client import HttpClient
from stac_asset.planetary_computer_client import PlanetaryComputerClient
from tqdm import tqdm
from tempfile import TemporaryDirectory


class Downloader:
    def __init__(self, item_coll, assets=None, out_dir=None):
        """
        Initialize the downloader with the item collection to download.

        Args:
            item_coll (pystac.ItemCollection or list of pystac.item.Item):
                The item collection to download.
            assets (list): A list of asset keys to download. Defaults to None,
                which downloads all assets.
            out_dir (str): The directory to download the files to. If not specified,
                a new directory will be created with the current timestamp.
        """
        self.item_coll = item_coll
        self.assets = assets
        if not out_dir:
            self.out_dir = f"data_{datetime.now().strftime('%H%M%S')}"
        else:
            self.out_dir = out_dir

    async def run(self):
        """
        Executes the download processes followed by clean-up routines.
        """
        await self._async_download()
        self._remove_empty_items(self.out_dir)

    async def _async_download(self, pre_n=10):
        """
        Download the items in the item collection to the output directory asynchronously.

        Args:
            assets (list): A list of asset keys to download.
            pre_n (int): The number of items to download for the preview run.
                Used to estimate the size of the download.
        """
        # set up download parameters
        opt_retry = ExponentialRetry(attempts=3)
        opt_timeout = ClientTimeout(total=3600)
        stac_config = dict(warn=True)
        if self.assets:
            stac_config["include"] = self.assets
        stac_config = stac_asset.Config(**stac_config)

        # preview run to estimate size
        if len(self.item_coll) >= pre_n:
            print("Estimating size of download...")
            np.random.seed(42)
            pre_coll = np.random.choice(self.item_coll, size=pre_n, replace=False)
            pre_coll = ItemCollection(items=pre_coll)

            with TemporaryDirectory() as temp_dir:
                # Perform the download
                await stac_asset.download_item_collection(
                    item_collection=pre_coll,
                    directory=temp_dir,
                    keep_non_downloaded=False,
                    config=stac_config,
                    clients=[
                        HttpClient(
                            RetryClient(
                                ClientSession(timeout=opt_timeout),
                                retry_options=opt_retry,
                            )
                        ),
                        PlanetaryComputerClient(
                            RetryClient(
                                ClientSession(timeout=opt_timeout),
                                retry_options=opt_retry,
                            )
                        ),
                    ],
                )

                # clean directory
                self._remove_empty_items(temp_dir)
                n_items = len(os.listdir(temp_dir)) - 1

                # evaluate size
                if n_items == pre_n:
                    mean_size = (
                        Downloader._get_dir_size(temp_dir) / pre_n * len(self.item_coll)
                    )
                    sub_dirs = [os.path.join(temp_dir, x.id) for x in pre_coll]
                    std_size = np.std([Downloader._get_dir_size(x) for x in sub_dirs])
                    ci_size = (
                        1.96 * std_size / ((pre_n - 1) ** 0.5) * len(self.item_coll)
                    )
                    print(
                        f"Estimated total size: {Downloader._sizeof_fmt(mean_size)} \xb1 "
                        f"{Downloader._sizeof_fmt(ci_size)} (95% confidence interval)"
                    )
                else:
                    print("Not enough items to estimate size. Skipping preview run.")
        else:
            print("Not enough items to estimate size. Skipping preview run.")

        # Starting the progress bar / message handler
        messages = asyncio.Queue()
        message_handler_task = asyncio.create_task(
            self._async_message_handling(messages, len(self.item_coll), self.out_dir)
        )

        # Downloading the item collection
        await stac_asset.download_item_collection(
            item_collection=self.item_coll,
            directory=self.out_dir,
            keep_non_downloaded=False,
            config=stac_config,
            clients=[
                HttpClient(
                    RetryClient(
                        ClientSession(timeout=opt_timeout), retry_options=opt_retry
                    )
                ),
                PlanetaryComputerClient(
                    RetryClient(
                        ClientSession(timeout=opt_timeout), retry_options=opt_retry
                    )
                ),
            ],
            messages=messages,
        )

        # Signal the message handler to stop
        await messages.put(None)
        await message_handler_task

    async def _async_message_handling(
        self, messages, total_files, directory, interval=1
    ):
        """
        Handle messages from the download process and update progress bars.

        Args:
            messages (asyncio.Queue): The queue to receive messages from the download process.
            total_files (int): The total number of files to download.
            directory (str): The directory where the files are being downloaded.
            interval (int): The interval in seconds at which to update the progress bars.
        """
        size_bar = tqdm(
            total=None,
            desc="Downloading EO data",
            unit="B",
            unit_scale=True,
            unit_divisor=1024,
            miniters=1,
        )
        # variables to keep track of progress
        last_checked = time.time()
        current_size = 0
        prev_size = 0

        while True:
            message = await messages.get()
            # check finished
            if message is None:
                break
            # update every interval seconds
            current_time = time.time()
            if current_time - last_checked >= interval:
                current_size = Downloader._get_dir_size(directory)
                last_checked = current_time
                size_bar.update(current_size - prev_size)
                prev_size = current_size

        # close the progress bars when done
        size_bar.close()

    def _remove_empty_items(self, out_path):
        """
        Remove empty item directories from the output directory.

        Args:
            out_path (str): The path to the output directory.
        """
        # find empty item dirs & delete them
        empty_dirs = Downloader._find_empty_subdirs(out_path)
        for dir in empty_dirs:
            shutil.rmtree(dir)
        # update item collection
        coll_path = os.path.join(out_path, "item-collection.json")
        in_coll = pystac.item_collection.ItemCollection.from_file(coll_path)
        all_items = [x.id for x in in_coll.items]
        rm_items = [os.path.split(x)[-1] for x in empty_dirs]
        for id in rm_items:
            all_items.remove(id)
        keep_items = [x for x in in_coll.items if x.id in all_items]
        out_coll = pystac.item_collection.ItemCollection(items=keep_items)
        # write back to file
        out_coll.save_object(coll_path)

    @staticmethod
    def _find_empty_subdirs(directory):
        """
        Return a list of empty subdirectories within the given directory.

        Args:
            directory (str): The directory to search for empty subdirectories.
        """
        empty_dirs = []
        for dirpath, dirnames, filenames in os.walk(directory):
            if not dirnames and not filenames:
                empty_dirs.append(dirpath)
            for dirname in list(dirnames):
                full_path = os.path.join(dirpath, dirname)
                if not os.listdir(full_path):
                    empty_dirs.append(full_path)
                    dirnames.remove(dirname)
        return empty_dirs

    @staticmethod
    def _get_dir_size(directory):
        """Calculate the total size of files in the specified directory.

        Args:
            directory (str): The path to the directory whose size is to be calculated.

        Returns:
            int: Total size of files in the directory in bytes.
        """
        total_size = 0
        for dirpath, dirnames, filenames in os.walk(directory):
            for f in filenames:
                fp = os.path.join(dirpath, f)
                # skip if it is symbolic link
                if not os.path.islink(fp):
                    total_size += os.path.getsize(fp)
        return total_size

    @staticmethod
    def _sizeof_fmt(num, suffix="B"):
        """
        Convert a number of bytes to a human-readable format.

        Args:
            num (int): The number of bytes.
            suffix (str): The suffix to use for the unit.
        """
        for unit in ("", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"):
            if abs(num) < 1024.0:
                return f"{num:3.0f}{unit}{suffix}"
            num /= 1024.0
        return f"{num:.0f}Yi{suffix}"
