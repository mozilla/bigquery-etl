"""BigQuery ETL."""

import warnings
from pathlib import Path

from .config import ConfigLoader

# We need to set the project dir before this module is imported from cli.__init__.py
if __name__ == "bigquery_etl":
    ConfigLoader.set_project_dir(Path().absolute())

# Ignore annoying warnings from Google code (https://github.com/googleapis/google-cloud-python/issues/13974).
# TODO: Remove this filter when that Google Python SDK issue is resolved (or maybe once setuptools v81 is released).
warnings.filterwarnings("ignore", "pkg_resources is deprecated as an API")
