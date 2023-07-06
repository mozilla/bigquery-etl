# Generated via https://github.com/mozilla/bigquery-etl/blob/main/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskMarker
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
import datetime
from utils.constants import ALLOWED_STATES, FAILED_STATES
from utils.gcp import bigquery_etl_query, gke_command, bigquery_dq_check

docs = """

Derived tables built on Adjust data downloaded from https://api.adjust.com/kpis/v1/<app_token>".
app_tokens are stored in the Airflow variable ADJUST_APP_TOKEN_LIST

API token used is Marlene's personal API token
#### Owner

mhirose@mozilla.com
"""


default_args = {
    "owner": "mhirose@mozilla.com",
    "start_date": datetime.datetime(2023, 7, 6, 0, 0),
    "end_date": None,
    "email": ["telemetry-alerts@mozilla.com", "mhirose@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
}

tags = ["impact/tier_2"]

with DAG(
    "bqetl_adjust_derived",
    default_args=default_args,
    schedule_interval="0 4 * * *",
    doc_md=docs,
    tags=tags,
) as dag:
    adjust_derived__adjust_derived__v1 = gke_command(
        task_id="adjust_derived__adjust_derived__v__download",
        command=[
            "python",
            "sql/moz-fx-data-shared-prod/adjust_derived/adjust_derived_v1/query.py",
        ]
        + [],
        docker_image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
        owner="mhirose@mozilla.com",
        email=["mhirose@mozilla.com", "telemetry-alerts@mozilla.com"],
    )

    adjust_derived_v1 = bigquery_etl_query(
        task_id="adjust_derived_v1",
        destination_table="adjust_derived_v1",
        dataset_id="adjust_derived",
        project_id="moz-fx-data-shared-prod",
        date_partition_parameter="submission_date:DATE:{{ds}}",
        depends_on_past=False,
        task_concurrency=1,
    )
