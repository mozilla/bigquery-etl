# Generated via https://github.com/mozilla/bigquery-etl/blob/main/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskMarker
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
import datetime
from utils.constants import ALLOWED_STATES, FAILED_STATES
from utils.gcp import bigquery_etl_query, gke_command, bigquery_dq_check

docs = """
### bqetl_accounts_backend_external

Built from bigquery-etl repo, [`dags/bqetl_accounts_backend_external.py`](https://github.com/mozilla/bigquery-etl/blob/main/dags/bqetl_accounts_backend_external.py)

#### Description

Copies data from Firefox Accounts (FxA) CloudSQL databases.

This DAG is under active development.

#### Owner

akomar@mozilla.com
"""


default_args = {
    "owner": "akomar@mozilla.com",
    "start_date": datetime.datetime(2023, 9, 19, 0, 0),
    "end_date": None,
    "email": ["akomar@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=600),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
}

tags = ["impact/tier_3", "repo/bigquery-etl", "triage/no_triage"]

with DAG(
    "bqetl_accounts_backend_external",
    default_args=default_args,
    schedule_interval="30 1 * * *",
    doc_md=docs,
    tags=tags,
) as dag:
    accounts_backend_external__nonprod_accounts__v1 = bigquery_etl_query(
        task_id="accounts_backend_external__nonprod_accounts__v1",
        destination_table="nonprod_accounts_v1",
        dataset_id="accounts_backend_external",
        project_id="moz-fx-data-shared-prod",
        owner="akomar@mozilla.com",
        email=["akomar@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=True,
    )
