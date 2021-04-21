"""Scripts for downloading data from http://web.antaq.gov.br/Anuario/."""

from os import mkdir, path, remove
from urllib import request
from logging import info, error
import zipfile

from .exceptions import DownloadError


def get_data(year: int):
    """Get extracted data for a specific year and saving ir to a folder."""
    base_url = "http://web.antaq.gov.br/Sistemas/ArquivosAnuario/Arquivos"
    url = base_url + f"/{year}.zip"
    zip_path = f"data/{year}.zip"
    data_dir = f"data/{year}/"

    # Only download the data if there is no such stored at data folder
    if not path.isdir(data_dir):
        info(f'Downloading data files for {year}')
        try:
            request.urlretrieve(url, zip_path)
        except Exception as e:
            error(f'Could not download data for year {year}. Reason: {e}')
            raise DownloadError(e)
        else:
            # Extract data to a folder and remove the zip file
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(data_dir)
            
            remove(zip_path)

            info(f'Data downloaded and stored at "{data_dir}"')
    else:
        info(f'Path "{data_dir}" already exist, files will not ' \
            + 'be downloaded')
        
