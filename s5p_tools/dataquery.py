"""Set of tools to query Copernicus database."""

from multiprocessing.pool import ThreadPool
from os import makedirs, rename
from os.path import exists

from sentinelsat.sentinel import (InvalidChecksumError, SentinelAPI,
                                  SentinelAPIError, geojson_to_wkt,
                                  read_geojson)
from tqdm import tqdm


def query_copernicus_hub(aoi=None,
                         username='s5pguest',
                         password='s5pguest',
                         hub='https://s5phub.copernicus.eu/dhus',
                         **kwargs):
    """Query Copernicus Open access Hub.

    :param aoi: (str) Geojson Area of interest url
    :param username: (str) Username to use for API connection
    :param password: (str) Password to use for API connection
    :param hub: (str) Url of hub to query
    :param kwargs: (dict) extra keywords for the api.query function (see https://sentinelsat.readthedocs.io/en/stable/cli.html#sentinelsat)
    :return: (SentinelAPI, dict) API object and results of query
    """
    # connect to the API
    api = SentinelAPI(username, password, hub)

    # query database
    if aoi is None:
        products = api.query(**kwargs)
    else:
        # convert .geojson file
        footprint = geojson_to_wkt(read_geojson(aoi))
        products = api.query(footprint, **kwargs)

    # display results
    tqdm.write(('Number of products found: {number_product}\n'
                'Total products size: {size:.2f} GB\n'
                ).format(number_product=len(products),
                         size=api.get_products_size(products)))

    return api, products


def get_filenames_request(products, download_directory='L2_data'):
    """Get local files url corresponding to a Copernicus request (must be already downloaded).

    :param products: (dict) Copernicus Hub query
    :param download_directory: (str) Url of folder for downloaded products
    :return: (list) List of strings with local urls for each product in the request
    """
    # list of id's per requested products
    ids_request = list(products.keys())

    # list of downloaded filenames urls
    filenames = [
        f"{download_directory}/{products[file_id]['title']}.nc" for file_id in ids_request]

    return filenames


def request_copernicus_hub(aoi=None,
                           login='s5pguest',
                           password='s5pguest',
                           hub='https://s5phub.copernicus.eu/dhus',
                           download_directory='L2_data',
                           checksum=True,
                           fix_extension=True,
                           num_threads=4,
                           **kwargs):
    """Query Copernicus Open access Hub and download automatically files that are not already downloaded.

    :param aoi: (str) Geojson Area of interest url
    :param username: (str) Username to use for API connection
    :param password: (str) Password to use for API connection
    :param hub: (str) Url of hub to query
    :param download_directory: (str) Url of folder for downloaded products
    :param checksum: (bool) Verify product integrity after download
    :param fix_extension: (bool) Fix extension from .zip to .nc (see https://github.com/sentinelsat/sentinelsat/issues/270)
    :param num_threads: (int) Number of parallel threads
    :param kwargs: (dict) extra keywords for the api.query function (see https://sentinelsat.readthedocs.io/en/stable/cli.html#sentinelsat)
    :return: (SentinelAPI, dict) API object and results of query
    """
    api, products = query_copernicus_hub(aoi, login, password, hub, **kwargs)
    ids_request = list(products.keys())
    makedirs(download_directory, exist_ok=True)

    # Set of free tqdm bar slot
    free_bars = list(range(num_threads))

    def _fetch_product(file_id):
        api = SentinelAPI(login, password, hub)
        bar_position = free_bars.pop(0)
        api._tqdm = lambda **kwargs: tqdm(position=bar_position,
                                          leave=False, **kwargs)

        if not exists(f"{download_directory}/{products[file_id]['title']}.nc"):
            # file not already downloaded
            tqdm.write(
                f"File {file_id} not found. Downloading into {download_directory}")
            try:
                api.get_product_odata(file_id)
            except SentinelAPIError:
                tqdm.write(f"Error: File {file_id} not found in Hub. Skipping")
            else:
                while True:
                    try:
                        api.download(file_id,
                                     directory_path=download_directory,
                                     checksum=checksum)
                    except InvalidChecksumError:
                        tqdm.write((f"Invalid checksum error in {file_id}. "
                                    "Trying again..."))
                        continue
                    else:
                        # fix .zip extention
                        if fix_extension:
                            rename(f"{download_directory}/{products[file_id]['title']}.zip",
                                   f"{download_directory}/{products[file_id]['title']}.nc")
                        tqdm.write(f"File {file_id} successfully downloaded")
                        break

        else:
            tqdm.write(f"File {file_id} already exists")

        # Free the bar slot
        free_bars.append(bar_position)
        return None

    tqdm.write(f"Launched {num_threads} threads")
    with ThreadPool(num_threads) as pool:
        pool.map_async(_fetch_product, ids_request)
        pool.close()
        pool.join()

    tqdm.write("\n")
    return api, products
