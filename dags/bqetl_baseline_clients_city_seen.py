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
### bqetl_baseline_clients_city_seen

Built from bigquery-etl repo, [`dags/bqetl_baseline_clients_city_seen.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_baseline_clients_city_seen.py)

#### Description

Scheduled queries for client city seen tables
#### Owner

wichan@mozilla.com

#### Tags

* impact/tier_2
* repo/bigquery-etl
"""


default_args = {
    "owner": "wichan@mozilla.com",
    "start_date": datetime.datetime(2025, 9, 20, 0, 0),
    "end_date": None,
    "email": ["telemetry-alerts@mozilla.com", "wichan@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
    "max_active_tis_per_dag": None,
}

tags = ["impact/tier_2", "repo/bigquery-etl"]

with DAG(
    "bqetl_baseline_clients_city_seen",
    default_args=default_args,
    schedule_interval="0 4 * * *",
    doc_md=docs,
    tags=tags,
    catchup=False,
) as dag:

    fenix_baseline_clients_city_seen_v1 = bigquery_etl_query(
        task_id="fenix_baseline_clients_city_seen_v1",
        destination_table="baseline_clients_city_seen_v1",
        dataset_id="fenix_derived",
        project_id="moz-fx-data-shared-prod",
        owner="wichan@mozilla.com",
        email=["telemetry-alerts@mozilla.com", "wichan@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=True,
        parameters=["submission_date:DATE:{{ds}}"],
    )

    firefox_desktop_baseline_clients_city_seen_v1 = bigquery_etl_query(
        task_id="firefox_desktop_baseline_clients_city_seen_v1",
        destination_table="baseline_clients_city_seen_v1",
        dataset_id="firefox_desktop_derived",
        project_id="moz-fx-data-shared-prod",
        owner="wichan@mozilla.com",
        email=["telemetry-alerts@mozilla.com", "wichan@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=True,
        parameters=["submission_date:DATE:{{ds}}"],
    )
