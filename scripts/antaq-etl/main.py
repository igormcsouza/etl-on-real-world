from logging import basicConfig, DEBUG

from data_extraction.download_files import get_data
from data_transformation.transforme_data import (
    create_atracacao_fato, create_carga_fato)
from data_extraction.exceptions import DownloadError


# Configuring logging file and format
basicConfig(
    filename='project.log', 
    level=DEBUG, 
    format='%(asctime)s - %(levelname)s - %(message)s', 
    datefmt='%m/%d/%Y %I:%M:%S %p'
)

# Get data for the specific years of interest
years_of_interest = [2018, 2019, 2020]

for year in years_of_interest:
    try:
        get_data(year)
    except DownloadError as de:
        # If there was an error downloading a year, the script will skip it and
        # continue for next years.
        continue

# Transform data according to the needs
create_atracacao_fato(years_of_interest)
create_carga_fato(years_of_interest)