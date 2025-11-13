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
### bqetl_braze_monitor_plus_users_sync

Built from bigquery-etl repo, [`dags/bqetl_braze_monitor_plus_users_sync.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_braze_monitor_plus_users_sync.py)

#### Description

Create Monitor Plus Users sync table to sync to Braze.
#### Owner

sherrera@mozilla.com

#### Tags

* impact/tier_2
* repo/bigquery-etl
"""


default_args = {
    "owner": "sherrera@mozilla.com",
    "start_date": datetime.datetime(2025, 11, 13, 0, 0),
    "end_date": None,
    "email": ["telemetry-alerts@mozilla.com", "sherrera@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
    "max_active_tis_per_dag": None,
}

tags = ["impact/tier_2", "repo/bigquery-etl"]

with DAG(
    "bqetl_braze_monitor_plus_users_sync",
    default_args=default_args,
    schedule_interval="@once",
    doc_md=docs,
    tags=tags,
    catchup=False,
) as dag:

    braze_external__monitor_plus_sync__v1 = bigquery_etl_query(
        task_id="braze_external__monitor_plus_sync__v1",
        destination_table="monitor_plus_sync_v1",
        dataset_id="braze_external",
        project_id="moz-fx-data-shared-prod",
        owner="sherrera@mozilla.com",
        email=[
            "cbeck@mozilla.com",
            "sherrera@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )
