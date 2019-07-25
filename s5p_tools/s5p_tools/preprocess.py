from os import makedirs
from os.path import exists

import xarray as xr
import harp
from json import load
import numpy as np
from datetime import datetime, timedelta
from itertools import product
import cartopy.io.shapereader as shpreader
import shapely.vectorized
from shapely.ops import cascaded_union

from ..pretty_print import printRed, printCyan, printBold


def _geojson_coordinates(geojsonurl):
    """
    Compute the coordinates of points a geojson file

    :param geojsonurl: (str) Geojson url
    :return: (list) List of coordinates
    """

    # load geojson file
    data = load(open(geojsonurl))

    # create the list of coordinates
    if data['features'][0]['geometry']['type'] == "MultiPolygon":
        list_coordinates = np.array(
            [component[0] for component in data['features'][0]['geometry']['coordinates']])

    else:
        list_coordinates = np.array(
            data['features'][0]['geometry']['coordinates'])

    return list_coordinates


def geojson_window(geojsonurl):
    """
    Compute the extent of a geojson file

    :param geojsonurl: (str) Geojson url
    :return: (list) Extent
    """

    # create the list of coordinates
    list_coordinates = _geojson_coordinates(geojsonurl)

    # compute map window
    min_coordinates_lons = list_coordinates[0][:, 0].min()
    min_coordinates_lats = list_coordinates[0][:, 1].min()

    max_coordinates_lons = list_coordinates[0][:, 0].max()
    max_coordinates_lats = list_coordinates[0][:, 1].max()

    extent = [min_coordinates_lons, max_coordinates_lons,
              min_coordinates_lats, max_coordinates_lats]

    return extent


def convert_to_l3_products(filenames, pre_commands='', post_commands='', export_path='L3_data'):
    """
    Process L2 products and convert to L3 using harpconvert

    :param filenames: (list) List of urls of L2 products
    :param pre_commands: (str) Harp command used during import of L2 products
    :param post_commands: (str) Harp command used during export of L3 products
    :param export_path: (str) Url of folder for converted products
    """

    makedirs(export_path, exist_ok=True)

    for filename in filenames:

        if not exists("{export_path}/{name}".format(export_path=export_path,
                                                    name=filename.split('/')[-1].replace('L2', 'L3'))):

            print("Converting {filename}".format(filename=filename))
            if exists(filename):
                try:
                    output_product = harp.import_product(
                        filename, operations=pre_commands)
                    export_url = "{export_path}/{name}".format(export_path=export_path,
                                                               name=filename.split('/')[-1].replace('L2', 'L3'))
                    harp.export_product(
                        output_product, export_url, file_format='netcdf', operations=post_commands)
                except harp._harppy.NoDataError:
                    printRed("Exception occured in {filename}: Product contains no variables or variables without data".format(
                        filename=filename))
            else:
                printRed('File {filename} not found'.format(
                    filename=filename))
        else:
            print("File {export_path}/{name} already exists".format(
                export_path=export_path, name=filename.split('/')[-1].replace('L2', 'L3')))


def make_country_mask(shapefile_url, lons, lats):
    """
    Create a mask filtering pixels outside a given country

    :param shapefile_url: (str) Url to shapefile
    :param lons: (DataArray) Xarray of longitudes
    :param lats: (DataArray) Xarray of latitudes
    :return: (array) Numpy mask
    """

    points = list(product(lats.values, lons.values))
    x = [i for j, i in points]
    y = [j for j, i in points]

    reader = shpreader.Reader(shapefile_url)
    records = list(reader.records())

    # Create mask by fusioning all geometries Polygons in shapefile
    global_geometry = cascaded_union([area.geometry for area in records])
    mask = shapely.vectorized.contains(global_geometry, x, y)

    return mask.reshape((lats.shape[0], lons.shape[0]))
