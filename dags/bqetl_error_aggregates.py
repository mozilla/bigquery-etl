# Generated via https://github.com/mozilla/bigquery-etl/blob/main/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskMarker
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
import datetime
from utils.constants import ALLOWED_STATES, FAILED_STATES
from utils.gcp import bigquery_etl_query, gke_command, bigquery_dq_check

docs = """
### bqetl_error_aggregates

Built from bigquery-etl repo, [`dags/bqetl_error_aggregates.py`](https://github.com/mozilla/bigquery-etl/blob/main/dags/bqetl_error_aggregates.py)

#### Owner

wkahngreene@mozilla.com
"""


default_args = {
    "owner": "wkahngreene@mozilla.com",
    "start_date": datetime.datetime(2019, 11, 1, 0, 0),
    "end_date": None,
    "email": ["telemetry-alerts@mozilla.com", "wkahngreene@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1200),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
}

tags = ["impact/tier_1", "repo/bigquery-etl"]

with DAG(
    "bqetl_error_aggregates",
    default_args=default_args,
    schedule_interval=datetime.timedelta(seconds=10800),
    doc_md=docs,
    tags=tags,
) as dag:
    telemetry_derived__error_aggregates__v1 = bigquery_etl_query(
        task_id="telemetry_derived__error_aggregates__v1",
        destination_table="error_aggregates_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="wkahngreene@mozilla.com",
        email=["telemetry-alerts@mozilla.com", "wkahngreene@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )
