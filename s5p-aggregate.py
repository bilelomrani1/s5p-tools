import argparse
import sys
import warnings
from os import makedirs
from pathlib import Path

import geopandas
import numpy as np
import rioxarray


def main(raster_path, shp_path, col_name, agg_func, export_dir):
    raster = rioxarray.open_rasterio(raster_path)
    shp = geopandas.read_file(shp_path).to_crs("EPSG:4326")

    if col_name is None:
        col_name = f"{agg_func}_raster"

    shp[col_name] = shp.apply(lambda row: raster.rio.clip(
        [row.geometry], shp.crs).reduce(eval(f'np.nan{agg_func}')).values.item(0), axis=1)

    export_path = export_dir / agg_func
    makedirs(export_path, exist_ok=True)
    shp.to_file(f"{export_path}/{raster_path.stem}__{shp_path.stem}.shp")


if __name__ == "__main__":

    # Ignore warnings
    warnings.filterwarnings("ignore", category=RuntimeWarning)
    warnings.filterwarnings("ignore", category=FutureWarning)

    # CLI ARGUMENTS

    parser = argparse.ArgumentParser(
        description=('Export a shapefile with a column containing aggregated'
                     'data extracted from an input raster file')
    )

    parser.add_argument(
        'raster', help='path to the a raster tif file', type=str)

    parser.add_argument(
        'shp', help='path to the shapefile', type=str)

    parser.add_argument(
        '--col-name', help='name of the column to be created in the shapefile', type=str)

    parser.add_argument('--agg-func', help='aggregation function',
                        type=str, default='mean')

    args = parser.parse_args()

    # PATHS

    # export: directory for shapefile
    EXPORT_DIR = 'aggregated'

    main(raster_path=Path(args.raster),
         shp_path=Path(args.shp),
         col_name=args.col_name,
         agg_func=args.agg_func,
         export_dir=Path(EXPORT_DIR))
