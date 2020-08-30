import argparse
from os.path import exists
from os import makedirs
import warnings

from tqdm import tqdm
from pathos.multiprocessing import cpu_count
import xarray as xr
import rioxarray
import geopandas
from shapely.geometry import mapping
import numpy as np
import pandas as pd

from s5p_tools import bounding_box, convert_to_l3_products, request_copernicus_hub, get_filenames_request


def main(product, aoi, date, qa, unit, resolution, command, shp, chunk_size, num_threads, num_workers):

    tqdm.write('\nRequest products\n')

    _, products = request_copernicus_hub(login=DHUS_USER,
                                         password=DHUS_PASSWORD,
                                         hub=DHUS_URL,
                                         aoi=aoi,
                                         date=date,
                                         platformname='Sentinel-5 Precursor',
                                         producttype=product,
                                         download_directory=f'{DOWNLOAD_DIR}/{product}',
                                         checksum=CHECKSUM,
                                         num_threads=num_threads)

    L2_files_urls = get_filenames_request(
        products, f'{DOWNLOAD_DIR}/{product}')

    if len(L2_files_urls) == 0:
        tqdm.write('Done\n')
        exit(0)

    # PREPROCESS DATA

    tqdm.write('Convert into L3 products\n')

    # harpconvert commands :
    # the source data is filtered + binning data by latitude/longitude

    harp_filter_commands = {

        'L2__NO2___': (f'tropospheric_NO2_column_number_density_validity>={qa};'
                       'tropospheric_NO2_column_number_density>=0;'
                       'NO2_column_number_density>=0;'
                       'stratospheric_NO2_column_number_density>=0;'
                       'NO2_slant_column_number_density>=0'),

        'L2__O3____': (f'O3_column_number_density_validity>={qa};'
                       'O3_column_number_density>=0'),

        'L2__SO2___': (f'SO2_column_number_density_validity>={qa};'
                       'SO2_column_number_density>=0;'
                       'SO2_slant_column_number_density>=0;'
                       'O3_column_number_density>=0'),

        'L2__HCHO__': (f'tropospheric_HCHO_column_number_density_validity>={qa};'
                       'tropospheric_HCHO_column_number_density>=0;'
                       'HCHO_slant_column_number_density>=0'),

        'L2__CO____': (f'CO_column_number_density_validity>={qa};'
                       'H2O_column_number_density>=0'),

        'L2__CH4___': (f'CH4_column_volume_mixing_ratio_dry_air_validity>={qa};'
                       'H2O_column_number_density>=0'),

        'L2__AER_AI': (f'absorbing_aerosol_index_validity>={qa}'),

        'L2__CLOUD_': (f'cloud_fraction_validity>={qa};'
                       'cloud_fraction>=0')

    }

    harp_keep_commands = {

        'L2__NO2___': ('keep(NO2_column_number_density,tropospheric_NO2_column_number_density,'
                       'stratospheric_NO2_column_number_density,NO2_slant_column_number_density,'
                       'tropopause_pressure,absorbing_aerosol_index,cloud_fraction,datetime_start,'
                       'longitude,latitude)'),

        'L2__O3____': ('keep(O3_column_number_density,O3_column_number_density_amf,O3_slant_column_number_density,'
                       'O3_effective_temperature,cloud_fraction,datetime_start,longitude,latitude)'),

        'L2__SO2___': ('keep(SO2_column_number_density,SO2_column_number_density_amf,SO2_slant_column_number_density,'
                       'absorbing_aerosol_index,cloud_fraction,datetime_start,longitude,latitude)'),

        'L2__HCHO__': ('keep(tropospheric_HCHO_column_number_density,tropospheric_HCHO_column_number_density_amf,'
                       'HCHO_slant_column_number_density,cloud_fraction,datetime_start,longitude,latitude)'),

        'L2__CO____': ('keep(CO_column_number_density,H2O_column_number_density,cloud_height,datetime_start,'
                       'longitude,latitude)'),

        'L2__CH4___': ('keep(CH4_column_volume_mixing_ratio_dry_air, aerosol_height,'
                       'aerosol_optical_depth,datetime_start,longitude,latitude)'),

        'L2__AER_AI': 'keep(absorbing_aerosol_index,datetime_start,longitude,latitude)',

        'L2__CLOUD_': ('keep(cloud_fraction,cloud_top_pressure,cloud_top_height,cloud_base_pressure,'
                       'cloud_base_height,cloud_optical_depth,surface_albedo,datetime_start,'
                       'longitude,latitude)'),

    }

    harp_conversion_commands = {

        'L2__NO2___': (f'derive(tropospheric_NO2_column_number_density [{unit}]);'
                       f'derive(stratospheric_NO2_column_number_density [{unit}]);'
                       f'derive(NO2_column_number_density [{unit}]);'
                       f'derive(NO2_slant_column_number_density [{unit}])'),

        'L2__O3____': (f'derive(O3_column_number_density [{unit}])'),

        'L2__SO2___': (f'derive(SO2_column_number_density [{unit}]);'
                       f'derive(SO2_slant_column_number_density [{unit}]);'
                       f'derive(O3_column_number_density [{unit}])'),

        'L2__HCHO__': (f'derive(tropospheric_HCHO_column_number_density [{unit}]);'
                       f'derive(HCHO_slant_column_number_density [{unit}])'),

        'L2__CO____': (f'derive(CO_column_number_density [{unit}]);'
                       f'derive(H2O_column_number_density [{unit}])'),

        'L2__CH4___': (f'derive(H2O_column_number_density [{unit}]);'
                       f'derive(dry_air_column_number_density [{unit}])'),

        'L2__AER_AI': '',

        'L2__CLOUD_': ''

    }

    # Step size for spatial re-gridding (in degrees)
    lon_step, lat_step = resolution

    if aoi is None:
        extent = [-180, 180, -90, 90]
    else:
        extent = bounding_box(aoi)

    # computes offsets and number of samples
    lat_edge_length = int(abs(extent[3] - extent[2]) / lat_step + 1)
    lat_edge_offset = extent[2]
    lon_edge_length = int(abs(extent[1] - extent[0]) / lon_step + 1)
    lon_edge_offset = extent[0]

    # create HARP commands
    if command is None:
        pre_commands = ''

        if unit is None:
            pre_commands = f'{harp_filter_commands[product]}'
        else:
            pre_commands = f'{harp_filter_commands[product]};{harp_conversion_commands[product]}'

        harp_commands = (f'{pre_commands};derive(datetime_stop {{time}});'
                         f'bin_spatial({lat_edge_length},{lat_edge_offset},{lat_step},'
                         f'{lon_edge_length},{lon_edge_offset},{lon_step});'
                         f'derive(latitude {{latitude}});derive(longitude {{longitude}});'
                         f'{harp_keep_commands[product]}')

    else:
        harp_commands = command

    # perform conversion
    convert_to_l3_products(L2_files_urls,
                           pre_commands=harp_commands,
                           export_path=f"{EXPORT_DIR}/{product.replace('L2', 'L3')}",
                           num_workers=num_workers)

    # Recover attributes
    attributes = {
        filename.split('/')[-1]: {
            'time_coverage_start': xr.open_dataset(filename).attrs['time_coverage_start'],
            'time_coverage_end': xr.open_dataset(filename).attrs['time_coverage_end'],
        } for filename in L2_files_urls
    }

    # AGGREGATE DATASET

    tqdm.write('Process data\n')

    # Avoid lost attributes during conversion
    xr.set_options(keep_attrs=True)

    def preprocess(ds):
        ds['time'] = pd.to_datetime(
            np.array([attributes[ds.attrs['source_product']]['time_coverage_start']])).values
        return ds

    DS = xr.open_mfdataset([filename.replace('L2', 'L3') for filename in L2_files_urls
                            if exists(filename.replace('L2', 'L3'))],
                           combine='nested',
                           concat_dim='time',
                           preprocess=preprocess,
                           decode_times=False,
                           chunks={'time': chunk_size})
    DS = DS.sortby('time')
    DS.rio.write_crs("epsg:4326", inplace=True)
    DS.rio.set_spatial_dims(x_dim='longitude', y_dim='latitude', inplace=True)
    DS = DS.drop_vars('datetime_start')

    # APPLY SHAPEFILE

    if shp is not None:
        tqdm.write("Applying shapefile\n")
        shapefile = geopandas.read_file(shp).to_crs("EPSG:4326")
        shapefile.geometry = shapefile.geometry.simplify(min(resolution)/2)
        DS = DS.rio.clip(shapefile.geometry.apply(mapping), shapefile.crs, drop=False)


    # EXPORT DATASET

    tqdm.write('Export dataset\n')

    start = min([products[uuid]['beginposition'] for uuid in products.keys()])
    end = max([products[uuid]['endposition'] for uuid in products.keys()])
    makedirs(f'{PROCESSED_DIR}/processed{product[2:]}', exist_ok=True)
    file_export_name = (f'{PROCESSED_DIR}/processed{product[2:]}/'
                        f'{product[4:]}{start.day}-{start.month}-{start.year}'
                        f'__{end.day}-{end.month}-{end.year}.nc')

    DS.to_netcdf(file_export_name)

    tqdm.write('Done\n')


if __name__ == "__main__":

    # Ignore warnings
    warnings.filterwarnings("ignore", category=RuntimeWarning)

    # PARAMS

    # Perform checksum verification after each download
    CHECKSUM = True

    # CLI ARGUMENTS

    parser = argparse.ArgumentParser(
        description=('Request, download and process Sentinel data from Copernicus access hub. '
                     'Create a processed netCDF file binned by time, latitude and longitude')
    )

    # Product type: Used to perform a product based search
    # Possible values are
    #   L2__O3____
    #   L2__NO2___
    #   L2__SO2___
    #   L2__CO____
    #   L2__CH4___
    #   L2__HCHO__
    #   L2__AER_AI
    #   L2__CLOUD_
    parser.add_argument('product', help='Product type', type=str)

    # Date: Used to perform a time interval search
    # The general form to be used is:
    #       date=(<timestamp>, <timestamp>)
    # where < timestamp > can be expressed in one of the following formats:
    #   yyyyMMdd
    #   yyyy-MM-ddThh:mm:ssZ
    #   yyyy-MM-ddThh:mm:ss.SSSZ(ISO8601 format)
    #   NOW
    #   NOW-<n>MINUTE(S)
    #   NOW-<n>HOUR(S)
    #   NOW-<n>DAY(S)
    #   NOW-<n>MONTH(S)
    parser.add_argument('--date', help='date used to perform a time interval search',
                        nargs=2, type=str, default=('NOW-24HOURS', 'NOW'))

    # Area of interest: The url of the area of interest (.geojson)
    parser.add_argument(
        '--aoi', help='path to the area of interest (.geojson)', type=str)

    # Shapefile: The url of the shapefile (.shp) for pixel filtering
    parser.add_argument(
        '--shp', help='path to the shapefile (.shp) for masking', type=str)

    # Harp command: Harp convert command used during import of products
    parser.add_argument(
        '--command', help='harp convert command used during import of products', type=str)

    # Unit: Unit conversion
    parser.add_argument('--unit', help='unit conversion', type=str)

    # qa value: Quality value threshold
    parser.add_argument('--qa', help='quality value threshold',
                        type=int, default=75)

    # resolution: Spatial resolution in arc degrees
    parser.add_argument('--resolution', help='spatial resolution in arc degrees', nargs=2,
                        type=float, default=(0.01, 0.01))

    # chunk-size:
    parser.add_argument('--chunk-size', help='dask chunk size along the time dimension',
                        type=int, default=200)

    # num-threads:
    parser.add_argument('--num-threads', help='number of threads spawned for L2 download',
                        type=int, default=4)

    # num-workers:
    parser.add_argument('--num-workers', help='number of workers spawned for L3 conversion',
                        type=int, default=cpu_count())

    args = parser.parse_args()

    # PATHS

    # download_directory: directory for L2 products
    DOWNLOAD_DIR = 'L2_data'

    # export_directory: directory for L3 products
    EXPORT_DIR = 'L3_data'

    # processed_directory: directory for processed products (aggregated+masked)
    PROCESSED_DIR = 'processed'

    # CREDENTIALS

    DHUS_USER = 's5pguest'
    DHUS_PASSWORD = 's5pguest'
    DHUS_URL = 'https://s5phub.copernicus.eu/dhus'

    main(product=args.product,
         aoi=args.aoi,
         date=args.date,
         qa=args.qa,
         unit=args.unit,
         resolution=args.resolution,
         command=args.command,
         shp=args.shp,
         chunk_size=args.chunk_size,
         num_threads=args.num_threads,
         num_workers=args.num_workers)
