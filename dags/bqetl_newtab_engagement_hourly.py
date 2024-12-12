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
### bqetl_newtab_engagement_hourly

Built from bigquery-etl repo, [`dags/bqetl_newtab_engagement_hourly.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_newtab_engagement_hourly.py)

#### Description

This DAG aggregates Newtab engagement data - on an hourly cadence

#### Owner

cbeck@mozilla.com

#### Tags

* impact/tier_2
* repo/bigquery-etl
"""


default_args = {
    "owner": "cbeck@mozilla.com",
    "start_date": datetime.datetime(2024, 12, 9, 0, 0),
    "end_date": None,
    "email": ["cbeck@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=300),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
}

tags = ["impact/tier_2", "repo/bigquery-etl"]

with DAG(
    "bqetl_newtab_engagement_hourly",
    default_args=default_args,
    schedule_interval="@hourly",
    doc_md=docs,
    tags=tags,
) as dag:

    telemetry_derived__newtab_desktop_daily_engagement_by_tile_id_position__v1 = bigquery_etl_query(
        task_id="telemetry_derived__newtab_desktop_daily_engagement_by_tile_id_position__v1",
        destination_table='newtab_desktop_daily_engagement_by_tile_id_position_v1${{ (execution_date - macros.timedelta(hours=1)).strftime("%Y%m%d") }}',
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="cbeck@mozilla.com",
        email=["cbeck@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        parameters=[
            "submission_date:DATE:{{ (execution_date - macros.timedelta(hours=1)).strftime('%Y-%m-%d') }}"
        ],
        sql_file_path="sql/moz-fx-data-shared-prod/ads_derived/newtab_desktop_daily_engagement_by_tile_id_position/query.sql",
    )
