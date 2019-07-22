"""
Set of tools to query Copernicus database.
"""

from os import listdir, rename, makedirs
from os.path import exists

from sentinelsat.sentinel import SentinelAPI, read_geojson, geojson_to_wkt
from ..pretty_print import printBold, printCyan, printRed


def query_copernicus_hub(aoi=None, username='s5pguest', password='s5pguest', hub='https://s5phub.copernicus.eu/dhus', **kwargs):
    """
    Query Copernicus Open access Hub.

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
    printBold('Results:\n {} products found\n'.format(len(products)))

    return api, products


def download_copernicus_hub(products, username='s5pguest', password='s5pguest', hub='https://s5phub.copernicus.eu/dhus',
                            download_directory='L2_data', checksum=True, fix_extension=True):
    """
    Download products from Copernicus Hub.

    :param products: (dict) Copernicus Hub query
    :param username: (str) Username to use for API connection
    :param password: (str) Password to use for API connection
    :param hub: (str) Url of hub to query
    :param download_directory: (str) Url of folder for downloaded products
    :param checksum: (bool) Verify product integrity after download
    :param fix_extension: (bool) Fix extension from .zip to .nc (see https://github.com/sentinelsat/sentinelsat/issues/270)
    :return: (SentinelAPI, dict) API object and results of download
    """

    # connect to the API
    api = SentinelAPI(username, password, hub)

    # download products
    makedirs(download_directory, exist_ok=True)
    download_res, _, _ = api.download_all(
        products, directory_path=download_directory, checksum=checksum)

    # list of id's per requested products
    ids_download = list(download_res.keys())

    if fix_extension:
        # fix .zip extention
        for file_id in ids_download:
            if exists("{download_directory}/{name}.zip".format(download_directory=download_directory,
                                                               name=download_res[file_id]['title'])):
                rename("{download_directory}/{name}.zip".format(download_directory=download_directory,
                                                                name=download_res[file_id]['title']),
                       "{download_directory}/{name}.nc".format(download_directory=download_directory,
                                                               name=download_res[file_id]['title']))

    printCyan('Done')

    return api, download_res


def get_filenames_request(products, download_directory='L2_data'):
    """
    Get local files url corresponding to a Copernicus request (must be already downloaded)

    :param products: (dict) Copernicus Hub query
    :param download_directory: (str) Url of folder for downloaded products
    :return: (list) List of strings with local urls for each product in the request
    """

    # list of id's per requested products
    ids_request = list(products.keys())

    # list of downloaded filenames urls
    filenames = ["{download_directory}/{name}.nc".format(download_directory=download_directory,
                                                         name=products[file_id]['title']) for file_id in ids_request]

    return filenames


def request_copernicus_hub(aoi=None, login='s5pguest', password='s5pguest', hub='https://s5phub.copernicus.eu/dhus',
                           download_directory='L2_data', checksum=True, fix_extension=True, **kwargs):
    """
    Query Copernicus Open access Hub and download automatically files that are not already downloaded.

    :param aoi: (str) Geojson Area of interest url
    :param username: (str) Username to use for API connection
    :param password: (str) Password to use for API connection
    :param hub: (str) Url of hub to query
    :param download_directory: (str) Url of folder for downloaded products
    :param checksum: (bool) Verify product integrity after download
    :param fix_extension: (bool) Fix extension from .zip to .nc (see https://github.com/sentinelsat/sentinelsat/issues/270)
    :param kwargs: (dict) extra keywords for the api.query function (see https://sentinelsat.readthedocs.io/en/stable/cli.html#sentinelsat)
    :return: (SentinelAPI, dict) API object and results of query
    """

    api, products = query_copernicus_hub(aoi, login, password, hub, **kwargs)
    ids_request = list(products.keys())
    makedirs(download_directory, exist_ok=True)

    for file_id in ids_request:
        if not exists("{download_directory}/{name}.nc".format(download_directory=download_directory,
                                                              name=products[file_id]['title'])):

            # file not already downloaded
            print(("File {name} not found. "
                   "Downloading into {download_directory}").format(name=file_id, download_directory=download_directory))

            try:
                api.get_product_odata(file_id)
            except:
                printRed(
                    "Error: File {name} not found in Hub. Skipping".format(name=file_id))
            else:
                try:
                    api.download(
                        file_id, directory_path=download_directory, checksum=checksum)
                except:
                    printRed("Error during download")
                else:
                    # fix .zip extention
                    if fix_extension:
                        rename("{download_directory}/{name}.zip".format(download_directory=download_directory,
                                                                        name=products[file_id]['title']),
                               "{download_directory}/{name}.nc".format(download_directory=download_directory,
                                                                       name=products[file_id]['title']))

        else:
            print("File {name} already exists".format(name=file_id))

    return api, products
