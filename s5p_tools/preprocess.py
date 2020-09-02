from functools import partial
from multiprocessing import Pool
from os import makedirs
from os.path import exists

import geopandas
import harp
from tqdm import tqdm


def bounding_box(geojsonurl):
    """Compute the extent of a geojson file.

    :param geojsonurl: (str) Geojson url
    :return: (list) Extent
    """
    minx, miny, maxx, maxy = geopandas.read_file(
        geojsonurl).bounds.values.squeeze()
    return [minx, maxx, miny, maxy]


def _process_file(filename, pre_commands, post_commands, export_path):

    # write does not work until https://github.com/tqdm/tqdm/issues/680 is solved

    if not exists(export_path / filename.name.replace('L2', 'L3')):

        # tqdm.write(f"Converting {filename}")
        if exists(filename):
            try:
                output_product = harp.import_product(str(filename),
                                                     operations=pre_commands)
                export_url = export_path / \
                    f"{filename.stem.replace('L2', 'L3')}.nc"
                harp.export_product(output_product,
                                    str(export_url),
                                    file_format='netcdf',
                                    operations=post_commands)
                # tqdm.write(f"{filename} successfully converted")

            except harp._harppy.NoDataError:
                pass
                # tqdm.write((f"Exception occured in {filename}: "
                #             "Product contains no variables or variables without data"))
        else:
            pass
            # tqdm.write(f'File {filename} not found')
    else:
        pass
        # tqdm.write("File {export_path}/{name} already exists".format(export_path=export_path,
        #  name=filename.split('/')[-1].replace('L2', 'L3')))

    return None


def convert_to_l3_products(filenames, pre_commands, post_commands, export_path, num_workers):
    """Process L2 products and convert to L3 using harpconvert.

    :param filenames: (list) List of urls of L2 products
    :param pre_commands: (str) Harp command used during import of L2 products
    :param post_commands: (str) Harp command used during export of L3 products
    :param export_path: (str) Url of folder for converted products
    """
    makedirs(export_path, exist_ok=True)
    tqdm.write(f"Launched {num_workers} processes")

    with Pool(processes=num_workers) as pool:
        list(tqdm(pool.imap_unordered(partial(_process_file,
                                              pre_commands=pre_commands,
                                              post_commands=post_commands,
                                              export_path=export_path),
                                      filenames),
                  desc="Converting", leave=False, total=len(filenames)))
        pool.close()
        pool.join()
    tqdm.write("\n")
