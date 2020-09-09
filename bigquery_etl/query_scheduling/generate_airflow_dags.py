"""Generates Airflow DAGs for scheduled queries."""

import logging
import os
from argparse import ArgumentParser
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
    "--output_dir",
    "--output-dir",
    default=DEFAULT_DAGS_DIR,
    help="Generated DAGs are written to this output directory.",
)
standard_args.add_log_level(parser)


def get_dags(sql_dir, dags_config):
    """Return all configured DAGs including associated tasks."""
    tasks = []
    dag_collection = DagCollection.from_file(dags_config)

    # parse metadata.yaml to retrieve scheduling information
    if os.path.isdir(sql_dir):
        for root, dirs, files in os.walk(sql_dir):
            try:
                if QUERY_FILE in files:
                    query_file = os.path.join(root, QUERY_FILE)
                    task = Task.of_query(query_file, dag_collection=dag_collection)
                elif QUERY_PART_FILE in files:
                    # multipart query
                    query_file = os.path.join(root, QUERY_PART_FILE)
                    task = Task.of_multipart_query(
                        query_file, dag_collection=dag_collection
                    )
                else:
                    continue
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
            except Exception as e:
                # in the case that there was some other error, report the query
                # that failed before exiting
                logging.error(f"Error processing task for query {query_file}")
                raise e
            else:
                tasks.append(task)

    else:
        logging.error(
            """
            Invalid sql_dir: {}, sql_dir must be a directory with
            structure /<dataset>/<table>/metadata.yaml.
            """.format(
                sql_dir
            )
        )

    return dag_collection.with_tasks(tasks)


def main():
    """Generate Airflow DAGs."""
    args = parser.parse_args()
    dags_output_dir = Path(args.output_dir)

    dags = get_dags(args.sql_dir, args.dags_config)
    dags.to_airflow_dags(dags_output_dir)


if __name__ == "__main__":
    main()
