# Generated via https://github.com/mozilla/bigquery-etl/blob/main/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskMarker
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
import datetime
from operators.gcp_container_operator import GKEPodOperator
from utils.constants import ALLOWED_STATES, FAILED_STATES
from utils.gcp import bigquery_etl_query, bigquery_dq_check, bigquery_bigeye_check

docs = """
### bqetl_messaging_system_hourly

Built from bigquery-etl repo, [`dags/bqetl_messaging_system_hourly.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_messaging_system_hourly.py)

#### Description

Hourly tables for onboarding reporting
#### Owner

phlee@mozilla.com

#### Tags

* impact/tier_1
* repo/bigquery-etl
"""


default_args = {
    "owner": "phlee@mozilla.com",
    "start_date": datetime.datetime(2025, 12, 15, 0, 0),
    "end_date": None,
    "email": ["telemetry-alerts@mozilla.com", "phlee@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=600),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "max_active_tis_per_dag": None,
}

tags = ["impact/tier_1", "repo/bigquery-etl"]

with DAG(
    "bqetl_messaging_system_hourly",
    default_args=default_args,
    schedule_interval="@hourly",
    doc_md=docs,
    tags=tags,
    catchup=True,
) as dag:

    firefox_desktop_derived__onboarding_hourly__v2 = bigquery_etl_query(
        task_id="firefox_desktop_derived__onboarding_hourly__v2",
        destination_table='onboarding_hourly_v2${{ (execution_date - macros.timedelta(hours=1)).strftime("%Y%m%d") }}',
        dataset_id="firefox_desktop_derived",
        project_id="moz-fx-data-shared-prod",
        owner="phlee@mozilla.com",
        email=["phlee@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        parameters=[
            "submission_date:DATE:{{ (execution_date - macros.timedelta(hours=1)).strftime('%Y-%m-%d') }}"
        ],
    )
