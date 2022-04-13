import os
import logging
from typing import Optional

import yaml

logger = logging.getLogger(__name__)


CONFIG_PATH = 'configuration'


def load_config(file: str) -> Optional[dict]:
    full_path = os.path.join(os.getcwd(), CONFIG_PATH, file)
    if os.path.exists(full_path):
        with open(full_path, "rt") as f:
            config = yaml.safe_load(f.read())

        logger.info(f'Configuration from file {full_path} successfully loaded.')
        return config
    else:
        logger.error(f'Failed to load configuration from file {full_path}.')
        return None