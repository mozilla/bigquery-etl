"""
Generates Airflow DAGs from dags.yaml and scheduling information in metadata.yaml files
of queries.
"""

import logging
import os
from argparse import ArgumentParser
from ..util import standard_args
from bigquery_etl.query_scheduling.dag import Dags
from bigquery_etl.query_scheduling.task import Task, UnscheduledTask


DEFAULT_SQL_DIR = "sql/"
DEFAULT_DAGS_FILE = "dags.yaml"
QUERY_FILE = "query.sql"

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
                    logging.info(
                        f"Warning: no scheduling information for {query_file}."
                    )

    else:
        logging.error(
            """
            Invalid sql_dir: {}, sql_dir must be a directory with
            structure /<dataset>/<table>/metadata.yaml.
            """.format(
                sql_dir
            )
        )

    return Dags.from_file(dags_config).with_tasks(tasks)


def main():
    """Generate Airflow DAGs."""
    args = parser.parse_args()

    get_dags(args.sql_dir, args.dags_config)

    # todo: determine task upstream dependencies
    # todo: convert to Airflow DAG representation
    # todo: validate generated Airflow python
    # todo: write generated code to files


if __name__ == "__main__":
    main()
