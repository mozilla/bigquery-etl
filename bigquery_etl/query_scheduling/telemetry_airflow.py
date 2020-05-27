"""Contains helper methods to interface with the telemetry-airflow repository."""

import ast
from git import Repo
import os
from os.path import isfile, join
from pathlib import Path
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
    dag_ast = ast.parse(dag_content)

    dag_name = None

    for el in dag_ast.body:
        if el.__class__ == ast.Assign:
            if el.targets[0].__class__ == ast.Name:
                assignment = el.targets[0]

                # dag_name defined in variable
                if assignment.id == "dag_name":
                    dag_name = el.value.s

                # DAG defined using assignment
                if (
                    el.value.__class__ == ast.Call
                    and el.value.func.__class__ == ast.Name
                    and el.value.func.id == "DAG"
                ):
                    if el.value.args[0].__class__ == ast.Str:
                        dag_name = el.value.args[0].s

        # DAG defined using with statement
        if el.__class__ == ast.With:
            with_expr = el.items[0].context_expr

            if with_expr.func.__class__ == ast.Name and with_expr.func.id == "DAG":
                # check if DAG name defined inline as string
                if with_expr.args[0].__class__ == ast.Str:
                    dag_name = with_expr.args[0].s
            elif (
                with_expr.func.__class__ == ast.Attribute
                and with_expr.func.value.id == "models"
                and with_expr.func.attr == "DAG"
            ):
                if with_expr.args[0].__class__ == ast.Str:
                    dag_name = with_expr.args[0].s

    return dag_name


def _extract_tables_with_tasks(dag_content):
    """Extract BigQuery destination tables and corresponding tasks from an Airflow DAG definition."""
    pass


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

    return tasks_for_tables
