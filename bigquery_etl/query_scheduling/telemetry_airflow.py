"""Contains helper methods to interface with the telemetry-airflow repository."""

import ast
from git import Repo
import os
from os.path import isfile, join
from pathlib import Path
import re
import shutil
import tempfile


TELEMETRY_AIRFLOW_GITHUB = "https://github.com/mozilla/telemetry-airflow.git"


def download_repository():
    """Download the telemetry-airflow repository to a temporary directory."""
    tmp_dir = tempfile.gettempdir() + "/telemetry-airflow/"

    # the repository can only be cloned into an empty directory
    if os.path.exists(tmp_dir) and os.path.isdir(tmp_dir):
        shutil.rmtree(tmp_dir)

    Repo.clone_from(TELEMETRY_AIRFLOW_GITHUB, tmp_dir)

    airflow_dag_dir = tmp_dir + "/dags"
    return airflow_dag_dir


def _extract_dag_name(dag_content):
    """Extracts the DAG name for an Airflow DAG definition."""
    dag_name_regex = [
        # DAG name inline with DAG definition
        re.compile(
            r"DAG\([\n\r\s]*['\"](?P<dag>[^'\"]*)['\"][\n\r\s]*,.*\)", re.DOTALL
        ),
        # dag_name defined in variable
        re.compile(r"dag_name[\n\r\s]*=[\n\r\s]*['\"](?P<dag>[^'\"]*)['\"]", re.DOTALL),
    ]

    dag_name_matches = [re.findall(regex, dag_content) for regex in dag_name_regex]

    for match in dag_name_matches:
        if len(match) > 0:
            return match[0]

    return None


def _extract_tables_with_tasks(dag_content):
    """Extract BigQuery destination tables and corresponding tasks from an Airflow DAG definition."""
    dag_ast = ast.parse(dag_content)
    table_with_task = {}

    for el in dag_ast.body:
        print(ast.dump(el))

    return table_with_task


def get_tasks_for_tables(dag_dir):
    """
    Retrieve tasks and BigQuery destination tables from telemetry-airflow DAGs.

    Returns a Hashmap that the BigQuery table as the key and the DAG and task
    names as the values for which the table is set as destination_table.
    """
    tasks_for_tables = {}

    if os.path.isdir(dag_dir):
        dag_files = [
            f
            for f in os.listdir(dag_dir)
            if isfile(join(dag_dir, f)) and f.endswith("py")
        ]

        for dag_file in dag_files:
            dag_content = (Path(dag_dir) / Path(dag_file)).read_text().strip()

            dag_name = _extract_dag_name(dag_content)

            print(dag_file)
            print(dag_name)
            print()

            # tables_with_tasks = _extract_tables_with_tasks(dag_content)

    return tasks_for_tables
