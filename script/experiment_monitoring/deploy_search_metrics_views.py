"""Script for deploying experiment search metrics materialized views."""

from argparse import ArgumentParser
from google.cloud import bigquery
from pathlib import Path

import os

DATASETS = [
    "org_mozilla_fenix_derived",
    "telemetry_derived",
    "org_mozilla_firefox_beta_derived",
    "org_mozilla_firefox_derived"
]
METRICS = ["ad_clicks_count", "search_with_ads_count", "search_count"]

FILE_PATH = os.path.dirname(__file__)
BASE_DIR = Path(FILE_PATH).parent.parent
SQL_DIR = BASE_DIR / "sql" / "moz-fx-data-shared-prod"


def deploy_materialized_views(project):
    """Replace the materialized experiment search metrics views."""
    client = bigquery.Client()

    for dataset in DATASETS:
        for metric in METRICS:
            view_dir = SQL_DIR / dataset / f"experiment_{metric}_live_v1"
            view = view_dir / "init.sql"

            print(f"Deploy {view}")
            # delete existing materialized view
            client.delete_table(
                f"{project}.{dataset}.experiment_{metric}_live_v1", not_found_ok=True
            )
            # run SQL to create view
            client.query(view.read_text()).result()


if __name__ == "__main__":
    parser = ArgumentParser(description=__doc__)
    parser.add_argument(
        "--project", default="moz-fx-data-shared-prod", help="Target project"
    )
    args = parser.parse_args()
    deploy_materialized_views(args.project)
