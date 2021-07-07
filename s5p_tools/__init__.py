from pathlib import Path

from .utils import (
    compute_lengths_and_offsets,
    fetch_product,
    generate_harp_commands,
    preprocess_time,
    process_file,
)

DHUS_USER = "s5pguest"
DHUS_PASSWORD = "s5pguest"
DHUS_URL = "https://s5phub.copernicus.eu/dhus"

DOWNLOAD_DIR = Path("L2_data")
EXPORT_DIR = Path("L3_data")
PROCESSED_DIR = Path("processed")
