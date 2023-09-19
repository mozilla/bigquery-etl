"""BigQuery ETL."""
from pathlib import Path

from .config import ConfigLoader

# We need to set the project dir before this module is imported from cli.__init__.py
if __name__ == "bigquery_etl":
    ConfigLoader.set_project_dir(Path().absolute())
