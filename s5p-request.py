import argparse
from os.path import exists
from os import makedirs
import warnings

import xarray as xr
import numpy as np
import pandas as pd

from s5p_tools import geojson_window, convert_to_l3_products, request_copernicus_hub, get_filenames_request, make_country_mask


# Ignore warnings
warnings.filterwarnings("ignore", category=RuntimeWarning)


# -------------------------------------------------------------------
# -------------------------- PARAMS ---------------------------------
# -------------------------------------------------------------------

# Perform checksum verification after each download
CHECKSUM = True


# -------------------------------------------------------------------
# ------------------------- PARSE ARGUMENTS -------------------------
# -------------------------------------------------------------------

parser = argparse.ArgumentParser(
    description='Request, download and process Sentinel data from Copernicus access hub'
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
parser.add_argument('--date', help='Date', nargs='+',
                    type=str, default=('NOW-24HOURS', 'NOW'))


# Area of interest: The url of the area of interest (.geojson file)
parser.add_argument(
    '--aoi', help='The url of the area of interest (.geojson file)', type=str)


# Shapefile: The url of the shapefile (.shp) for pixel filtering
parser.add_argument(
    '--shp', help='The url of the shapefile (.shp) for pixel filtering', type=str)


# Harp command: Harp convert command used during import of products
parser.add_argument(
    '--command', help='Harp convert command used during import of products', type=str)


# Unit: Unit conversion
parser.add_argument('--unit', help='Unit conversion', type=str)


# qa value: Quality value threshold
parser.add_argument('--qa', help='Quality value threshold',
                    type=int, default=75)


args = parser.parse_args()


# -------------------------------------------------------------------
# ------------------------------ PATHS ------------------------------
# -------------------------------------------------------------------

# download_directory: directory for L2 products
DOWNLOAD_DIR = 'L2_data'

# export_directory: directory for L3 products
EXPORT_DIR = 'L3_data'

# processed_directory: directory for processed products (aggregated+masked)
PROCESSED_DIR = 'processed'


# -------------------------------------------------------------------
# ------------- QUERYING & DOWNLOADING PRODUCTS ---------------------
# -------------------------------------------------------------------

DHUS_USER = 's5pguest'
DHUS_PASSWORD = 's5pguest'
DHUS_URL = 'https://s5phub.copernicus.eu/dhus'

FIX_EXTENSION = True

print('\nRequest products\n')

_, products = request_copernicus_hub(login=DHUS_USER,
                                     password=DHUS_PASSWORD,
                                     hub=DHUS_URL,
                                     aoi=args.aoi,
                                     date=tuple(args.date),
                                     platformname='Sentinel-5 Precursor',
                                     producttype=args.product,
                                     download_directory=f'{DOWNLOAD_DIR}/{args.product}',
                                     checksum=CHECKSUM,
                                     fix_extension=FIX_EXTENSION)


L2_files_urls = get_filenames_request(
    products, f'{DOWNLOAD_DIR}/{args.product}')

if len(L2_files_urls) == 0:
    print('\nDone\n')
    exit(0)

# -------------------------------------------------------------------
# ----------------------- PREPROCESS DATA ---------------------------
# -------------------------------------------------------------------

print('\nConvert into L3 products\n')

# harpconvert commands :
# the source data is filtered + binning data by latitude/longitude

harp_filter_commands = {

    'L2__NO2___': (f'tropospheric_NO2_column_number_density_validity>{args.qa};'
                   'tropospheric_NO2_column_number_density>=0;'
                   'NO2_column_number_density>=0;'
                   'stratospheric_NO2_column_number_density>=0;'
                   'NO2_slant_column_number_density>=0'),

    'L2__O3____': (f'O3_column_number_density_validity>{args.qa};'
                   'O3_column_number_density>=0'),

    'L2__SO2___': (f'SO2_column_number_density_validity>{args.qa};'
                   'SO2_column_number_density>=0;'
                   'SO2_slant_column_number_density>=0;'
                   'O3_column_number_density>=0'),

    'L2__HCHO__': (f'tropospheric_HCHO_column_number_density_validity>{args.qa};'
                   'tropospheric_HCHO_column_number_density>=0;'
                   'HCHO_slant_column_number_density>=0'),

    'L2__CO____': (f'CO_column_number_density_validity>{args.qa};'
                   'H2O_column_number_density>=0'),

    'L2__CH4___': (f'CH4_column_volume_mixing_ratio_dry_air_validity>{args.qa};'
                   'H2O_column_number_density>=0'),

    'L2__AER_AI': (f'absorbing_aerosol_index_validity>{args.qa}'),

    'L2__CLOUD_': (f'cloud_fraction_validity>{args.qa};'
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

    'L2__NO2___': (f'derive(tropospheric_NO2_column_number_density [{args.unit}]);'
                   f'derive(stratospheric_NO2_column_number_density [{args.unit}]);'
                   f'derive(NO2_column_number_density [{args.unit}]);'
                   f'derive(NO2_slant_column_number_density [{args.unit}])'),

    'L2__O3____': (f'derive(O3_column_number_density [{args.unit}])'),

    'L2__SO2___': (f'derive(SO2_column_number_density [{args.unit}]);'
                   f'derive(SO2_slant_column_number_density [{args.unit}]);'
                   f'derive(O3_column_number_density [{args.unit}])'),

    'L2__HCHO__': (f'derive(tropospheric_HCHO_column_number_density [{args.unit}]);'
                   f'derive(HCHO_slant_column_number_density [{args.unit}])'),

    'L2__CO____': (f'derive(CO_column_number_density [{args.unit}]);'
                   f'derive(H2O_column_number_density [{args.unit}])'),

    'L2__CH4___': (f'derive(H2O_column_number_density [{args.unit}]);'
                   f'derive(dry_air_column_number_density [{args.unit}])'),

    'L2__AER_AI': '',

    'L2__CLOUD_': ''

}

# Step size for spatial re-gridding (in degrees)
LON_STEP = 0.01
LAT_STEP = 0.01

if args.aoi is None:
    extent = [-180, 180, -90, 90]
else:
    # compute map extent from aoi
    extent = geojson_window(args.aoi)

# computes offsets and number of samples
lat_edge_length = int(abs(extent[3] - extent[2]) / LAT_STEP + 1)
lat_edge_offset = extent[2]
lon_edge_length = int(abs(extent[1] - extent[0]) / LON_STEP + 1)
lon_edge_offset = extent[0]

if args.command is None:
    if args.unit is None:
        harp_commands = f'{harp_filter_commands[args.product]}'
    else:
        harp_commands = f'{harp_filter_commands[args.product]};{harp_conversion_commands[args.product]}'

    pre_commands = (f'{harp_commands};derive(datetime_stop {{time}});'
                    f'bin_spatial({lat_edge_length},{lat_edge_offset},{LAT_STEP},{lon_edge_length},{lon_edge_offset},'
                    f'{LON_STEP});derive(latitude {{latitude}});derive(longitude {{longitude}});'
                    f'{harp_keep_commands[args.product]}')

else:
    harp_commands = args.command
    pre_commands = (f'derive(datetime_stop {{time}});'
                    f'bin_spatial({lat_edge_length},{lat_edge_offset},{LAT_STEP},{lon_edge_length},{lon_edge_offset},'
                    f'{LON_STEP});derive(latitude {{latitude}});derive(longitude {{longitude}});'
                    f'{harp_keep_commands[args.product]}')

# perform conversion
convert_to_l3_products(L2_files_urls,
                       pre_commands=pre_commands,
                       export_path=f"{EXPORT_DIR}/{args.product.replace('L2', 'L3')}")

# -------------------------------------------------------------------
# ------------------- RECOVER ATTRIBUTES ----------------------------
# -------------------------------------------------------------------

attributes = {
    filename.split('/')[-1]: {
        'time_coverage_start': xr.open_dataset(filename).attrs['time_coverage_start'],
        'time_coverage_end': xr.open_dataset(filename).attrs['time_coverage_end'],
    } for filename in L2_files_urls
}

# -------------------------------------------------------------------
# ------------------ AGGREGRATE PRODUCTS ----------------------------
# -------------------------------------------------------------------

print('\nProcess data\n')

# Avoid lost attributes during conversion
xr.set_options(keep_attrs=True)

# open L2 products
print('Loading data\n')


def preprocess(ds):
    ds['time'] = pd.to_datetime(
        np.array([attributes[ds.attrs['source_product']]['time_coverage_start']])).values
    return ds


DS = xr.open_mfdataset([filename.replace('L2', 'L3') for filename in L2_files_urls
                        if exists(filename.replace('L2', 'L3'))],
                       combine='nested',
                       concat_dim='time',
                       preprocess=preprocess,
                       chunks={'time': 2000})
DS = DS.sortby('time')

# filter pixels
if args.shp is not None:
    print('\nApplying shapefile\n')
    mask = make_country_mask(args.shp, DS.longitude, DS.latitude)
    for column in [column_name for column_name in list(DS.variables)
                   if DS[column_name].dims == ('time', 'latitude', 'longitude')]:
        DS[column] = DS[column].where(mask)

# -------------------------------------------------------------------
# ---------------------- EXPORT PRODUCTS ----------------------------
# -------------------------------------------------------------------

print('Export dataset\n')

start = min([products[uuid]['beginposition'] for uuid in products.keys()])
end = max([products[uuid]['endposition'] for uuid in products.keys()])
makedirs(f'{PROCESSED_DIR}/processed{args.product[2:]}', exist_ok=True)
file_export_name = (f'{PROCESSED_DIR}/processed{args.product[2:]}/'
                    f'{args.product[4:]}{start.day}-{start.month}-{start.year}'
                    f'__{end.day}-{end.month}-{end.year}.nc')

DS.to_netcdf(file_export_name)

print('Done\n')
