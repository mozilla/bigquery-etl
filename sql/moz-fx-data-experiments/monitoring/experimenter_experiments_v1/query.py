#!/usr/bin/env python3

"""Import experiments from Experimenter via the Experimenter API."""

import datetime
import json
import logging
import sys
from argparse import ArgumentParser
from pathlib import Path

import cattrs
from google.cloud import bigquery

from bigquery_etl.experiments import get_experiments
from bigquery_etl.schema import SCHEMA_FILE, Schema

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

parser = ArgumentParser(description=__doc__)
parser.add_argument("--project", default="moz-fx-data-experiments")
parser.add_argument("--destination_dataset", default="monitoring")
parser.add_argument("--destination_table", default="experimenter_experiments_v1")
parser.add_argument("--metric_config_table", default="experiment_metric_configs_v1")
parser.add_argument("--sql_dir", default="sql/")
parser.add_argument("--dry_run", action="store_true")


def read_metric_configs(project: str, dataset: str, table: str) -> dict:
    """Get resolved metric configs from their table and join on normandy_slug.

    Log and skip in case of failure to read metric config table.
    """
    try:
        client = bigquery.Client(project)
        rows = client.query(
            f"SELECT normandy_slug, metric_config FROM `{project}.{dataset}.{table}`"
        ).result()
        return {row.normandy_slug: dict(row.metric_config) for row in rows}
    except Exception as e:
        logger.warning(f"Cannot read metric configs from {table}: {e}")
        return {}


def main():
    """Run."""
    args = parser.parse_args()
    experiments = get_experiments()

    destination_table = (
        f"{args.project}.{args.destination_dataset}.{args.destination_table}"
    )

    schema = Schema.from_schema_file(Path(__file__).parent / SCHEMA_FILE)

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.job.WriteDisposition.WRITE_TRUNCATE,
    )
    job_config.schema = schema.to_bigquery_schema()

    converter = cattrs.BaseConverter()
    converter.register_unstructure_hook(
        datetime.datetime, lambda d: datetime.datetime.strftime(d, format="%Y-%m-%d")
    )

    blob = converter.unstructure(experiments)

    metric_configs = read_metric_configs(
        args.project, args.destination_dataset, args.metric_config_table
    )
    for row in blob:
        row["metric_config"] = metric_configs.get(row["normandy_slug"])

    if args.dry_run:
        print(json.dumps(blob))
        sys.exit(0)

    client = bigquery.Client(args.project)
    client.load_table_from_json(blob, destination_table, job_config=job_config).result()
    logger.info(f"Loaded {len(experiments)} experiments")


if __name__ == "__main__":
    main()
