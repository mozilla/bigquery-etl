"""Generates Airflow DAGs for scheduled queries."""

import copy
import logging
import os
from argparse import ArgumentParser
from pathlib import Path

from bigquery_etl.query_scheduling.dag_collection import DagCollection
from bigquery_etl.query_scheduling.task import Task, TaskRef, UnscheduledTask
from bigquery_etl.util import standard_args
from bigquery_etl.util.common import project_dirs

DEFAULT_DAGS_FILE = "dags.yaml"
QUERY_FILE = "query.sql"
QUERY_PART_FILE = "part1.sql"
SCRIPT_FILE = "script.sql"
PYTHON_SCRIPT_FILE = "query.py"
DEFAULT_DAGS_DIR = "dags"
CHECKS_FILE = "checks.sql"

parser = ArgumentParser(description=__doc__)
parser.add_argument(
    "--dags_config",
    "--dags-config",
    help="File with DAGs configuration",
    default=DEFAULT_DAGS_FILE,
)
parser.add_argument(
    "--output_dir",
    "--output-dir",
    help="Generated DAGs are written to this output directory.",
    default=DEFAULT_DAGS_DIR,
)
parser.add_argument(
    "--dag_id",
    "--dag_id",
    help="The DAG to generate. Generates all DAGs by default",
    default=None,
)
parser.add_argument(
    "--project_id",
    "--project-id",
    help="Project to generate DAGs for. If not specified, generate for all projects.",
    default=None,
    required=False,
)

standard_args.add_log_level(parser)


def get_dags(project_id, dags_config):
    """Return all configured DAGs including associated tasks."""
    tasks = []
    dag_collection = DagCollection.from_file(dags_config)
    for project_dir in project_dirs(project_id):
        # parse metadata.yaml to retrieve scheduling information
        if os.path.isdir(project_dir):
            for root, dirs, files in os.walk(project_dir):
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
                    elif SCRIPT_FILE in files:
                        query_file = os.path.join(root, SCRIPT_FILE)
                        task = Task.of_script(query_file, dag_collection=dag_collection)
                    elif PYTHON_SCRIPT_FILE in files:
                        query_file = os.path.join(root, PYTHON_SCRIPT_FILE)
                        task = Task.of_python_script(
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
                    if CHECKS_FILE in files:
                        checks_file = os.path.join(root, CHECKS_FILE)
                        with open(checks_file, "r") as file:
                            file_contents = file.read()
                        # check if file contains fail and warn and create checks task accordingly
                        if "#fail" in file_contents:
                            is_check_task_fail = True
                            checks_fail_task = copy.deepcopy(
                                Task.of_dq_check(checks_file, is_check_task_fail,  dag_collection=dag_collection)
                            )
                            tasks.append(checks_fail_task)
                            task_ref = TaskRef(
                                dag_name=task.dag_name,
                                task_id=task.task_name,
                            )
                            checks_fail_task.upstream_dependencies.append(task_ref)
                        if "#warn" in file_contents:
                            print("warn in generate airflow")
                            is_check_task_fail = False
                            checks_warn_task = copy.deepcopy(
                                Task.of_dq_check(checks_file, is_check_task_fail,  dag_collection=dag_collection)
                            )

                            checks_warn_task.task_name = "checks__warn_down"
                            tasks.append(checks_warn_task)
                            task_ref = TaskRef(
                                dag_name=task.dag_name,
                                task_id=task.task_name,
                            )
                            checks_warn_task.upstream_dependencies.append(task_ref)
                    tasks.append(task)

        else:
            logging.error(
                """
                Invalid project_dir: {}, project_dir must be a directory with
                structure <sql>/<project>/<dataset>/<table>/metadata.yaml.
                """.format(
                    project_dir
                )
            )

    return dag_collection.with_tasks(tasks)


def main():
    """Generate Airflow DAGs."""
    args = parser.parse_args()
    dags_output_dir = Path(args.output_dir)

    dags = get_dags(args.project_id, args.dags_config)
    dags.to_airflow_dags(dags_output_dir, args.dag_id)


if __name__ == "__main__":
    main()
