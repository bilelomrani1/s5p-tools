from os import makedirs
from os.path import exists

import itertools
import harp
import numpy as np
import geopandas
from tqdm import tqdm
import pandas as pd
from pathos.multiprocessing import ProcessingPool as Pool, cpu_count


def bounding_box(geojsonurl):
    """
    Compute the extent of a geojson file

    :param geojsonurl: (str) Geojson url
    :return: (list) Extent
    """

    minx, miny, maxx, maxy = geopandas.read_file(geojsonurl).bounds.values.squeeze()
    return [minx, maxx, miny, maxy]


def convert_to_l3_products(filenames,
                           pre_commands='',
                           post_commands='',
                           export_path='L3_data',
                           num_workers=cpu_count()):
    """
    Process L2 products and convert to L3 using harpconvert

    :param filenames: (list) List of urls of L2 products
    :param pre_commands: (str) Harp command used during import of L2 products
    :param post_commands: (str) Harp command used during export of L3 products
    :param export_path: (str) Url of folder for converted products
    """

    def _process_file(filename):

        if not exists("{export_path}/{name}".format(export_path=export_path,
                                                    name=filename.split('/')[-1].replace('L2', 'L3'))):

            tqdm.write(f"Converting {filename}")
            if exists(filename):
                try:
                    output_product = harp.import_product(filename,
                                                         operations=pre_commands)
                    export_url = "{export_path}/{name}".format(export_path=export_path,
                                                               name=filename.split('/')[-1].replace('L2', 'L3'))
                    harp.export_product(output_product,
                                        export_url,
                                        file_format='netcdf',
                                        operations=post_commands)
                    tqdm.write(f"{filename} successfully converted")

                except harp._harppy.NoDataError:
                    tqdm.write((f"Exception occured in {filename}: "
                                "Product contains no variables or variables without data"))
            else:
                tqdm.write(f'File {filename} not found')
        else:
            tqdm.write("File {export_path}/{name} already exists".format(export_path=export_path,
                                                                         name=filename.split('/')[-1].replace('L2', 'L3')))

        return None

    makedirs(export_path, exist_ok=True)
    tqdm.write(f"Launched {num_workers} processes")
    with Pool(processes=num_workers) as pool:
        pool.uimap(_process_file, filenames)
        pool.close()
        pool.join()
    tqdm.write("\n")
