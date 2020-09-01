import argparse
from os import makedirs
from os.path import exists
import warnings

import pandas as pd
import numpy as np
from functools import partial
import geopandas
from shapely.geometry import mapping
from tqdm import tqdm
import xarray as xr
import rioxarray
from multiprocessing import Pool, cpu_count


def _export_raster(index, band_name, time_resolution, date_ranges, shapefile, ds, export_dir):

    export_name = (f"{export_dir}/{date_ranges[index]}.tif")

    if shapefile is not None:
        ds.isel(time=index).rio.clip(shapefile.geometry.apply(
            mapping), shapefile.crs).rio.to_raster(export_name)
    else:
        ds.isel(time=index).rio.to_raster(export_name)


def main(netcdf_file, time_resolution, shp, band_name, chunk_size, num_workers, agg_func, export_dir):

    tqdm.write("\n")

    # Check if netcdf_file exists
    if not exists(netcdf_file):
        tqdm.write(f"The file {netcdf_file} does not exist")
        exit(1)
    else:
        DS = xr.open_dataset(netcdf_file, chunks={'time': chunk_size})

    # Check if the band name is correct
    while True:
        try:
            band = DS[band_name]
            break
        except KeyError:
            tqdm.write(("The band name does not exist."
                        "The following bands were found:"))
            for variable in DS.data_vars.keys():
                tqdm.write(f'\t{variable}')
            band_name = input("Band name: ")

    # Check if the time resolution is correct
    frequency_mapping = {'D': "day",
                         'W': "week",
                         'M': "month",
                         'A': "year"}

    while True:
        try:
            ds = band.resample(time=time_resolution)
            break
        except ValueError:
            tqdm.write(("The frequency string must be of the form: "
                        "<n><freq> with <n> an integer and <freq> one "
                        "of the following:"))
            for freq_code, freq_name in frequency_mapping.items():
                tqdm.write(f"{freq_code}: {freq_name}")
            time_resolution = input("Enter a valid frequency string: ")

    # Check if agg_func is correct
    aggfunc_mapping = ['mean', 'median', 'sum', 'std', 'min', 'max']

    while True:
        try:
            ds = ds.reduce(eval(f'np.nan{agg_func}'))
            break
        except AttributeError:
            tqdm.write("The aggregation function string must "
                       "be one of the following:")
            for agg_func in aggfunc_mapping:
                tqdm.write(agg_func)
            agg_func = input("Enter a valid aggregation function string: ")

    if shp is not None:
        tqdm.write("Loading and simplifying shapefile...\n")

        # Compute the spatial resolution of the data
        delta_x, delta_y = abs(ds.x[1] - ds.x[0]), abs(ds.y[1] - ds.y[0])
        resolution = min(delta_x, delta_y).values.item(0) / 2

        # Load, reproject and simplify the geometries
        shapefile = geopandas.read_file(shp).to_crs("EPSG:4326")
        shapefile.geometry = shapefile.geometry.simplify(resolution)
    else:
        shapefile = None

    export_path = f'{export_dir}/{band_name}/{agg_func}'
    makedirs(export_path, exist_ok=True)
    num_time = len(ds.time)

    # Create date ranges
    frequency = time_resolution[-1]
    date_format_mapping = {'D': '%Y-%m-%d',
                           'W': '%Y-%m-week_%W',
                           'M': '%Y-%m',
                           'A': '%Y'}
    date_format = date_format_mapping[frequency]
    idx = pd.to_datetime(ds.time.values).to_period(time_resolution)
    date_ranges = ['{0}__{1}'.format(s, e) for s, e in zip(idx.asfreq(frequency, 's').strftime(date_format),
                                                           idx.asfreq(frequency, 'e').strftime(date_format))]

    with Pool(processes=num_workers) as pool:
        list(tqdm(pool.imap_unordered(partial(_export_raster,
                                              band_name=band_name,
                                              time_resolution=time_resolution,
                                              shapefile=shapefile,
                                              ds=ds,
                                              date_ranges=date_ranges,
                                              export_dir=export_path),
                                      range(num_time)), desc="Exporting", total=num_time))
        pool.close()
        pool.join()
    tqdm.write('\nDone\n')


if __name__ == "__main__":

    # Ignore warnings
    warnings.filterwarnings("ignore", category=RuntimeWarning)
    warnings.filterwarnings("ignore", category=FutureWarning)

    # CLI ARGUMENTS

    parser = argparse.ArgumentParser(
        description='Compress the processed netCDF file into multiple compressed files'
    )

    parser.add_argument(
        'netcdf', help='path to the processed netCDF file', type=str)

    parser.add_argument(
        'band', help='name of the band to export', type=str)

    parser.add_argument(
        '--time-resolution', help='resampling rate of the time dimension', type=str, default='1D')

    parser.add_argument(
        '--shp', help='path to the shapefile (.shp) for masking', type=str)

    # chunk-size:
    parser.add_argument('--chunk-size', help='dask chunk size along the time dimension',
                        type=int, default=200)

    # num-workers:
    parser.add_argument('--num-workers', help='number of workers spawned for compression',
                        type=int, default=cpu_count())

    # num-workers:
    parser.add_argument('--agg-func', help='aggregation function',
                        type=str, default='mean')

    args = parser.parse_args()

    # PATHS

    # export: directory for compressed files
    EXPORT_DIR = 'compressed'

    main(netcdf_file=args.netcdf,
         time_resolution=args.time_resolution,
         shp=args.shp,
         band_name=args.band,
         chunk_size=args.chunk_size,
         num_workers=args.num_workers,
         agg_func=args.agg_func,
         export_dir=EXPORT_DIR)
