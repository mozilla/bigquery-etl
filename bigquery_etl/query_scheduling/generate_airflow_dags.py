"""Generates Airflow DAGs for scheduled queries."""

import logging
import os
from argparse import ArgumentParser
from google.cloud import bigquery
from ..util import standard_args
from pathlib import Path

from bigquery_etl.query_scheduling.dag_collection import DagCollection
from bigquery_etl.query_scheduling.task import Task, UnscheduledTask


DEFAULT_SQL_DIR = "sql/"
DEFAULT_DAGS_FILE = "dags.yaml"
QUERY_FILE = "query.sql"
QUERY_PART_FILE = "part1.sql"
DEFAULT_DAGS_DIR = "dags"
TELEMETRY_AIRFLOW_GITHUB = "https://github.com/mozilla/telemetry-airflow.git"

parser = ArgumentParser(description=__doc__)
parser.add_argument(
    "--sql_dir",
    "--sql-dir",
    help="File or directory containing query and metadata files",
    default=DEFAULT_SQL_DIR,
)
parser.add_argument(
    "--dags_config",
    "--dags-config",
    help="File with DAGs configuration",
    default=DEFAULT_DAGS_FILE,
)
parser.add_argument(
    "--project_id",
    "--project-id",
    default="moz-fx-data-shared-prod",
    help="Dry run queries in this project to determine task dependencies.",
)
parser.add_argument(
    "--output_dir",
    "--output-dir",
    default=DEFAULT_DAGS_DIR,
    help="Generated DAGs are written to this output directory.",
)
standard_args.add_log_level(parser)


def get_dags(sql_dir, dags_config):
    """Return all configured DAGs including associated tasks."""
    tasks = []

    # parse metadata.yaml to retrieve scheduling information
    if os.path.isdir(sql_dir):
        for root, dirs, files in os.walk(sql_dir):
            if QUERY_FILE in files:
                query_file = os.path.join(root, QUERY_FILE)

                try:
                    task = Task.of_query(query_file)
                    tasks.append(task)
                except FileNotFoundError:
                    # query has no metadata.yaml file; skip
                    pass
                except UnscheduledTask:
                    # logging.debug(
                    #     f"No scheduling information for {query_file}."
                    # )
                    #
                    # most tasks lack scheduling information for now
                    pass
            elif QUERY_PART_FILE in files:
                # multipart query
                query_file = os.path.join(root, QUERY_PART_FILE)

                try:
                    task = Task.of_multipart_query(query_file)
                    tasks.append(task)
                except FileNotFoundError:
                    # query has no metadata.yaml file; skip
                    pass
                except UnscheduledTask:
                    pass

    else:
        logging.error(
            """
            Invalid sql_dir: {}, sql_dir must be a directory with
            structure /<dataset>/<table>/metadata.yaml.
            """.format(
                sql_dir
            )
        )

    return DagCollection.from_file(dags_config).with_tasks(tasks)


def main():
    """Generate Airflow DAGs."""
    args = parser.parse_args()
    client = bigquery.Client(args.project_id)
    dags_output_dir = Path(args.output_dir)

    dags = get_dags(args.sql_dir, args.dags_config)
    dags.to_airflow_dags(dags_output_dir, client)


if __name__ == "__main__":
    main()
