import harp
import numpy as np
from os.path import exists
import pandas as pd
from sentinelsat.sentinel import (
    InvalidChecksumError,
    SentinelAPIError,
)
from tqdm import tqdm


def compute_lengths_and_offsets(minx, miny, maxx, maxy, ystep, xstep):

    lat_edge_length = int(abs(maxy - miny) / ystep + 1)
    lat_edge_offset = miny
    lon_edge_length = int(abs(maxx - minx) / xstep + 1)
    lon_edge_offset = minx

    return lat_edge_length, lat_edge_offset, lon_edge_length, lon_edge_offset


def fetch_product(file_id, api, products, download_dir):

    if not exists(download_dir / f"{products[file_id]['title']}.nc"):

        tqdm.write(f"File {file_id} not found. Downloading into {download_dir}")

        try:
            api.get_product_odata(file_id)
        except SentinelAPIError:
            tqdm.write(f"Error: File {file_id} not found in Hub. Skipping")
        else:
            while True:
                try:
                    api.download(
                        file_id,
                        directory_path=download_dir,
                        checksum=True,
                    )
                except InvalidChecksumError:
                    tqdm.write(
                        (f"Invalid checksum error in {file_id}. "
                        "Trying again...")
                    )
                    continue
                else:
                    tqdm.write(f"File {file_id} successfully downloaded")
                    break
    else:
        tqdm.write(f"File {file_id} already exists")


def process_file(filename, harp_commands, export_dir):

    # write does not work until https://github.com/tqdm/tqdm/issues/680 is solved
    if not exists(export_dir / filename.name.replace("L2", "L3")):

        # tqdm.write(f"Converting {filename}")
        if exists(filename):
            try:
                output_product = harp.import_product(
                    str(filename), operations=harp_commands
                )
                export_url = export_dir / f"{filename.stem.replace('L2', 'L3')}.nc"
                harp.export_product(
                    output_product,
                    str(export_url),
                    file_format="netcdf"
                )
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


def preprocess_time(ds, attributes):
    ds["time"] = pd.to_datetime(
        np.array([attributes[ds.attrs["source_product"]]["time_coverage_start"]])
    ).values
    return ds


def generate_harp_commands(producttype,
                           qa,
                           unit,
                           lon_step,
                           lat_step,
                           lat_edge_length,
                           lat_edge_offset,
                           lon_edge_length,
                           lon_edge_offset):

    keep_general = [
        "latitude",
        "longitude",
        "sensor_altitude",
        "sensor_azimuth_angle",
        "sensor_zenith_angle",
        "solar_azimuth_angle",
        "solar_zenith_angle",
    ]

    harp_dict = {
        "L2__O3____": {
            "keep": [
                "O3_column_number_density",
                "O3_effective_temperature",
                "cloud_fraction",
            ],
            "filter": [f"O3_column_number_density_validity>={qa}"],
            "convert": [f"derive(O3_column_number_density [{unit}])"],
        },
        "L2__NO2___": {
            "keep": [
                "tropospheric_NO2_column_number_density",
                "NO2_column_number_density",
                "stratospheric_NO2_column_number_density",
                "NO2_slant_column_number_density",
                "tropopause_pressure",
                "absorbing_aerosol_index",
                "cloud_fraction",
            ],
            "filter": [
                f"tropospheric_NO2_column_number_density_validity>={qa}",
                "tropospheric_NO2_column_number_density>=0",
            ],
            "convert": [
                f"derive(tropospheric_NO2_column_number_density [{unit}])",
                f"derive(stratospheric_NO2_column_number_density [{unit}])",
                f"derive(NO2_column_number_density [{unit}])",
                f"derive(NO2_slant_column_number_density [{unit}])",
            ],
        },
        "L2__SO2___": {
            "keep": [
                "SO2_column_number_density",
                "SO2_column_number_density_amf",
                "SO2_slant_column_number_density",
                "cloud_fraction",
            ],
            "filter": [f"SO2_column_number_density_validity>={qa}"],
            "convert": [
                f"derive(SO2_column_number_density [{unit}])",
                f"derive(SO2_slant_column_number_density [{unit}])",
            ],
        },
        "L2__CO____": {
            "keep": ["CO_column_number_density", "H2O_column_number_density"],
            "filter": [f"CO_column_number_density_validity>={qa}"],
            "convert": [
                f"derive(CO_column_number_density [{unit}])",
                f"derive(H2O_column_number_density [{unit}])",
            ],
        },
        "L2__CH4___": {
            "keep": [
                "CH4_column_volume_mixing_ratio_dry_air",
                "aerosol_height",
                "aerosol_optical_depth",
                "cloud_fraction",
            ],
            "filter": [f"CH4_column_volume_mixing_ratio_dry_air_validity>={qa}"],
            "convert": [],
        },
        "L2__HCHO__": {
            "keep": [
                "tropospheric_HCHO_column_number_density",
                "tropospheric_HCHO_column_number_density_amf",
                "HCHO_slant_column_number_density",
                "cloud_fraction",
            ],
            "filter": [f"tropospheric_HCHO_column_number_density_validity>={qa}"],
            "convert": [
                f"derive(tropospheric_HCHO_column_number_density [{unit}])",
                f"derive(HCHO_slant_column_number_density [{unit}])",
            ],
        },
        "L2__CLOUD_": {
            "keep": [
                "cloud_fraction",
                "cloud_top_pressure",
                "cloud_top_height",
                "cloud_base_pressure",
                "cloud_base_height",
                "cloud_optical_depth",
                "surface_albedo",
            ],
            "filter": [f"cloud_fraction_validity>={qa}"],
            "convert": [],
        },
        "L2__AER_AI": {
            "keep": [
                "absorbing_aerosol_index",
            ],
            "filter": [f"absorbing_aerosol_index_validity>={qa}"],
            "convert": [],
        },
        "L2__AER_LH": {
            "keep": [
                "aerosol_height",
                "aerosol_pressure",
                "aerosol_optical_depth",
                "cloud_fraction",
            ],
            "filter": [f"aerosol_height_validity>={qa}"],
            "convert": [],
        },
    }

    return (
        ";".join(harp_dict[producttype]["filter"])
        + (";" if len(harp_dict[producttype]["filter"]) != 0 else "")
        + ";".join(harp_dict[producttype]["convert"])
        + (";" if len(harp_dict[producttype]["convert"]) != 0 else "")
        + "derive(datetime_stop {time});"
        + f"bin_spatial({lat_edge_length},{lat_edge_offset},{lat_step},{lon_edge_length},{lon_edge_offset},{lon_step});"
        + "derive(latitude {latitude});derive(longitude {longitude});"
        + f"keep({','.join(harp_dict[producttype]['keep'] + keep_general)})"
    )
