import os
import logging
import shutil

import requests

from exceptions.custom_exceptions import SkipDownloadButNoDb


logger = logging.getLogger(__name__)

DATA_BN_AUTHORITIES_DB_URL = "http://data.bn.org.pl/db/institutions/"


def create_url(source_db_name: str) -> str:
    return f"{DATA_BN_AUTHORITIES_DB_URL}{source_db_name}"


def get_raw_db(source_db_name: str, skip_download: bool) -> str:
    path_to_file = f"db/{source_db_name}"

    if skip_download:
        if not os.path.exists(path_to_file):
            raise SkipDownloadButNoDb
    else:
        if not os.path.exists("db"):
            os.makedirs("db")

        db_url = create_url(source_db_name)

        with requests.get(db_url, stream=True) as r:
            with open(path_to_file, 'wb') as fp:
                shutil.copyfileobj(r.raw, fp)

    return path_to_file
