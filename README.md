[![Build Status](https://travis-ci.com/bilelomrani1/s5p_tools.svg?branch=master)](https://travis-ci.com/bilelomrani/s5p_tools)
[![License](https://img.shields.io/github/license/bilelomrani1/s5p-tools.svg)](https://img.shields.io/github/license/bilelomrani1/s5p-tools.svg)

S5P-Tools
=====================================

A Python package to download, preprocess and plot data from [Copernicus Open Access Hub](https://scihub.copernicus.eu). This implementation is based on `sentinelsat` [package](https://github.com/sentinelsat/sentinelsat) and the [API Hub Access](https://scihub.copernicus.eu/twiki/do/view/SciHubWebPortal/APIHubDescription) to query the database. The preprocess is made with [HARP tools](https://cdn.rawgit.com/stcorp/harp/master/doc/html/harpconvert.html).

## Installation

The package can be installed via the following `conda` commands

```bash
conda config --add channels conda-forge
conda config --add channels stcorp
conda config --add channels bilelomrani

conda install -c conda-forge -c stcorp -c bilelomrani s5p_tools
```

## Downloading and processing data

### Quick start

The command `s5p-request` is used to query Copernicus Hub, download and process the data. The syntax is the following

```bash
s5p-request <product-type>
```
where `<product-type>` is a Sentinel-5P product. TROPOMI Level 2 geophysical products are given in the table below.

| Product type          | Parameter                                                         |
|-----------------------|-------------------------------------------------------------------|
| L2__O3____            | Ozone (O3) total column                                           |
| L2__NO2___            | Nitrogen Dioxide (NO2), tropospheric, stratospheric, slant column |
| L2__SO2___            | Sulfur Dioxide (SO2) total column                                 |
| L2__CO____            | Carbon Monoxide (CO) total column                                 |
| L2__CH4___            | Methane (CH4) total column                                        |
| L2__HCHO__            | Formaldehyde (HCHO) tropospheric, slant column                    |
| L2__AER_AI            | UV Aerosol Index                                                  |
| L2__CLOUD_            | Cloud fraction, albedo, top pressure                              |

By default, the script downloads all products corresponding to the specified product type for the last 24 hours. Custom date query can be specified via the option `--date`.

The resulting file is a `netCDF` file in the `processed` folder, binned by time, latitude, longitude, aligned on the same regular grid with resolution 0.01 x 0.01 arc degree.

### Options

The script `s5p-request` supports the following optional arguments:

#### Date

The option `--date` allows to specify a custom time range:

```bash
s5p-request <product-type> --date <timestamp> <timestamp>
```
where `<timestamp>` can be expressed in one of the following formats:
  - yyyyMMdd
  - yyyy-MM-ddThh:mm:ssZ
  - yyyy-MM-ddThh:mm:ss.SSSZ(ISO8601 format)
  - NOW
  - NOW-<n>MINUTE(S)
  - NOW-<n>HOUR(S)
  - NOW-<n>DAY(S)
  - NOW-<n>MONTH(S)

#### Area of interest

The option `--aoi` allows to specify a custom geographical area with a `geojson` file.

```bash
s5p-request <product-type> --aoi <geojson-file-url>
```

#### Shapefile

The option `--shp` allows to mask the resulting final dataset based on the geometry contained in a `.shp` shapefile.

```bash
s5p-request <product-type> --shp <shapefile-file-url>
```

If the shapefile contains more than one geometry, the script considers the union of all geometries. The script expects a shapefile encoded in `utf-8` and projected with Longitude / Latitude WGS84 projection. To standardize your `.shp` file, use the following `ogr2ogr` command from GDAL:

```bash
ogr2ogr -f "ESRI Shapefile" -lco ENCODING=UTF-8 -t_srs EPSG:4326 output.shp input.shp
```

#### Unit conversion

By default, no unit conversion is performed (SI units). To specify a custom unit conversion, use the option `--unit`.

```bash
s5p-request <product-type> --unit <unit>
```

Unit conversion supports the following arguments for densities:
- `molec/m2`
- `mol/m2` (default)

## Acknowledgements

The authors are grateful to the Luxembourg Institute of Socio-Economic Research (LISER) for funding this project. The authors would also like to acknowledge the European Spatial Agency for providing the API for the Sentinel 5P Hub. The content is solely the responsibility of the authors and does not necessarily represent the official views of the LISER.

If you use this code, please consider citing the following paper.

```
@article{OMRANI2020105089,
title = "Spatio-temporal data on the air pollutant nitrogen dioxide derived from Sentinel satellite for France",
journal = "Data in Brief",
volume = "28",
pages = "105089",
year = "2020",
issn = "2352-3409",
doi = "https://doi.org/10.1016/j.dib.2019.105089",
url = "http://www.sciencedirect.com/science/article/pii/S2352340919314453",
author = "Hichem Omrani and Bilel Omrani and Benoit Parmentier and Marco Helbich",
keywords = "Air pollution, Remote sensing, Monitoring",
abstract = "Monitoring of air pollution is an important task in public health. Availability of data is often hindered by the paucity of the ground monitoring station network. We present here a new spatio-temporal dataset collected and processed from the Sentinel 5P remote sensing platform. As an example application, we applied the full workflow to process measurements of nitrogen dioxide (NO2) collected over the territory of mainland France from May 2018 to June 2019. The data stack generated is daily measurements at a 4 × 7 km spatial resolution. The supplementary Python code package used to collect and process the data is made publicly available. The dataset provided in this article is of value for policy-makers and health assessment."
}
```
