"""Contains helper methods to interface with the telemetry-airflow repository."""

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
    airflow_functions_re = (
        "bigquery_etl_query|bigquery_etl_copy_deduplicate|bigquery_xcom_query"
    )

    function_calls = re.findall(
        re.compile(f"(?:{airflow_functions_re})\((.+?(?=\)\n))\)\n", re.DOTALL),
        dag_content,
    )

    table_with_task = {}

    for fn in function_calls:
        task_id = re.findall(
            r"task_id[\n\r\s]*=[\n\r\s]*['\"](?P<task>[^'\"]*)['\"][\n\r\s]*,", fn
        )
        table = re.findall(
            r"destination_table[\n\r\s]*=[\n\r\s]*['\"](?P<table>[^'\"]*)['\"][\n\r\s]*,",
            fn,
        )

        dataset_id = re.findall(
            r"dataset_id[\n\r\s]*=[\n\r\s]*['\"](?P<dataset>[^'\"]*)['\"][\n\r\s]*,",
            fn,
        )

        if len(table) > 0 and len(task_id) > 0 and len(dataset_id) > 0:
            table_with_task[dataset_id[0] + "." + table[0]] = task_id[0]

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
            tables_with_tasks = _extract_tables_with_tasks(dag_content)

            tables_with_dag_tasks = {
                table: {"task": task, "dag": dag_name}
                for table, task in tables_with_tasks.items()
            }

            print(tables_with_dag_tasks)

    return tasks_for_tables
