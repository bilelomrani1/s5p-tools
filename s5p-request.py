import argparse
from functools import partial
import geopandas
from multiprocessing.pool import ThreadPool, Pool
from os import cpu_count, makedirs
from os.path import exists
from pathlib import Path
import rioxarray
from sentinelsat.sentinel import (
    SentinelAPI,
    geojson_to_wkt,
    read_geojson,
)
import sys
from tqdm import tqdm
import xarray as xr

from s5p_tools import (
    compute_lengths_and_offsets,
    fetch_product,
    generate_harp_commands,
    preprocess_time,
    process_file,
    DHUS_USER,
    DHUS_PASSWORD,
    DHUS_URL,
    DOWNLOAD_DIR,
    EXPORT_DIR,
    PROCESSED_DIR
)


def main(
    producttype,
    aoi,
    date,
    qa,
    unit,
    resolution,
    chunk_size,
    num_threads,
    num_workers,
):

    api = SentinelAPI(DHUS_USER, DHUS_PASSWORD, DHUS_URL)

    tqdm.write("\nRequesting products\n")

    query_body = {
        "date": date,
        "platformname": "Sentinel-5 Precursor",
        "producttype": producttype
    }

    # query database
    if aoi is None:
        products = api.query(**query_body)
    else:
        footprint = geojson_to_wkt(read_geojson(Path(aoi)))
        products = api.query(footprint, **query_body)

    # display results
    tqdm.write(
        (
            "Number of products found: {number_product}\n"
            "Total products size: {size:.2f} GB\n"
        ).format(
            number_product=len(products),
            size=api.get_products_size(products)
            )
    )

    # list of uuids for each product in the query
    ids_request = list(products.keys())

    if len(ids_request) == 0:
        tqdm.write("Done!")
        sys.exit(0)

    # list of downloaded filenames urls
    filenames = [
        DOWNLOAD_DIR / f"{products[file_id]['title']}.nc"
        for file_id in ids_request
    ]

    makedirs(DOWNLOAD_DIR, exist_ok=True)

    with ThreadPool(num_threads) as pool:
        pool.map(
            partial(
                fetch_product,
                api=api,
                products=products,
                download_dir=DOWNLOAD_DIR
            ),
            ids_request)

        pool.close()
        pool.join()

    tqdm.write("Converting into L3 products\n")

    # Step size for spatial re-gridding (in degrees)
    xstep, ystep = resolution

    if aoi is None:
        minx, miny, maxx, maxy = -180, -90, 180, 90
    else:
        minx, miny, maxx, maxy = geopandas.read_file(Path(aoi)).bounds.values.squeeze()

    # computes offsets and number of samples
    lat_length, lat_offset, lon_length, lon_offset = compute_lengths_and_offsets(minx, miny, maxx, maxy, ystep, xstep)

    harp_commands = generate_harp_commands(
        producttype,
        qa,
        unit,
        xstep,
        ystep,
        lat_length,
        lat_offset,
        lon_length,
        lon_offset
    )

    makedirs(EXPORT_DIR, exist_ok=True)
    tqdm.write(f"Launched {num_workers} processes")

    with Pool(processes=num_workers) as pool:
        list(
            tqdm(
                pool.imap_unordered(
                    partial(
                        process_file,
                        harp_commands=harp_commands,
                        export_dir=EXPORT_DIR
                    ),
                    filenames,
                ),
                desc="Converting",
                leave=False,
                total=len(filenames),
            )
        )
        pool.close()
        pool.join()

    # Recover attributes
    attributes = {
        filename.name: {
            "time_coverage_start": xr.open_dataset(filename).attrs["time_coverage_start"],
            "time_coverage_end": xr.open_dataset(filename).attrs["time_coverage_end"],
        }
        for filename in filenames
    }

    tqdm.write("Processing data\n")
    xr.set_options(keep_attrs=True)

    DS = xr.open_mfdataset(
        [
            str(filename.relative_to(".")).replace("L2", "L3")
            for filename in filenames
            if exists(str(filename.relative_to(".")).replace("L2", "L3"))
        ],
        combine="nested",
        concat_dim="time",
        parallel=True,
        preprocess=partial(
            preprocess_time,
            attributes=attributes
            ),
        decode_times=False,
        chunks={"time": chunk_size},
    )

    DS = DS.sortby("time")
    DS.rio.write_crs("epsg:4326", inplace=True)
    DS.rio.set_spatial_dims(x_dim="longitude", y_dim="latitude", inplace=True)

    tqdm.write("Exporting netCDF file\n")

    start = min(products[uuid]["beginposition"] for uuid in products.keys())
    end = max(products[uuid]["endposition"] for uuid in products.keys())

    export_dir = PROCESSED_DIR / f"processed{producttype[2:]}"
    makedirs(export_dir, exist_ok=True)
    file_export_name = export_dir / (
        f"{producttype[4:]}{start.day}-{start.month}-{start.year}__"
        f"{end.day}-{end.month}-{end.year}.nc"
    )

    DS.to_netcdf(file_export_name)

    tqdm.write("Done!")


if __name__ == '__main__':

    parser = argparse.ArgumentParser(
        description=(
            "Request, download and process Sentinel data from Copernicus access hub. "
            "Create a processed netCDF file binned by time, latitude and longitude"
        )
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
    parser.add_argument("product", help="Product type", type=str)

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
    parser.add_argument(
        "--date",
        help="date used to perform a time interval search",
        nargs=2,
        type=str,
        default=("NOW-24HOURS", "NOW"),
    )

    # Area of interest: The url of the area of interest (.geojson)
    parser.add_argument(
        "--aoi", help="path to the area of interest (.geojson)", type=str
    )

    # Unit: Unit conversion
    parser.add_argument("--unit", help="unit conversion", type=str, default="mol/m2")

    # qa value: Quality value threshold
    parser.add_argument("--qa", help="quality value threshold", type=int, default=50)

    # resolution: Spatial resolution in arc degrees
    parser.add_argument(
        "--resolution",
        help="spatial resolution in arc degrees",
        nargs=2,
        type=float,
        default=(0.01, 0.01),
    )

    # chunk-size:
    parser.add_argument(
        "--chunk-size",
        help="dask chunk size along the time dimension",
        type=int,
        default=256,
    )

    # num-threads:
    parser.add_argument(
        "--num-threads",
        help="number of threads spawned for L2 download",
        type=int,
        default=4,
    )

    # num-workers:
    parser.add_argument(
        "--num-workers",
        help="number of workers spawned for L3 conversion",
        type=int,
        default=cpu_count(),
    )

    args = parser.parse_args()

    main(
        producttype=args.product,
        aoi=args.aoi,
        date=args.date,
        qa=args.qa,
        unit=args.unit,
        resolution=args.resolution,
        chunk_size=args.chunk_size,
        num_threads=args.num_threads,
        num_workers=args.num_workers,
    )
