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
### bqetl_ads_hourly

Built from bigquery-etl repo, [`dags/bqetl_ads_hourly.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_ads_hourly.py)

#### Description

Hourly tables for ad reporting
#### Owner

cbeck@mozilla.com

#### Tags

* impact/tier_3
* repo/bigquery-etl
* triage/confidential
"""


default_args = {
    "owner": "cbeck@mozilla.com",
    "start_date": datetime.datetime(2025, 7, 10, 0, 0),
    "end_date": None,
    "email": ["cbeck@mozilla.com", "telemetry-alerts@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=600),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
    "max_active_tis_per_dag": None,
}

tags = ["impact/tier_3", "repo/bigquery-etl", "triage/confidential"]

with DAG(
    "bqetl_ads_hourly",
    default_args=default_args,
    schedule_interval="@hourly",
    doc_md=docs,
    tags=tags,
    catchup=False,
) as dag:

    ads_derived__ads_client_operations_and_errors_hourly__v1 = bigquery_etl_query(
        task_id="ads_derived__ads_client_operations_and_errors_hourly__v1",
        destination_table='ads_client_operations_and_errors_hourly_v1${{ (execution_date - macros.timedelta(hours=1)).strftime("%Y%m%d") }}',
        dataset_id="ads_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ahanot@mozilla.com",
        email=[
            "ahanot@mozilla.com",
            "cbeck@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        parameters=[
            "submission_date:DATE:{{ (execution_date - macros.timedelta(hours=1)).strftime('%Y-%m-%d') }}"
        ],
        sql_file_path="sql/moz-fx-data-shared-prod/ads_derived/ads_client_operations_and_errors_hourly_v1/query.sql",
    )

    bigeye__ads_derived__ads_client_operations_and_errors_hourly__v1 = bigquery_bigeye_check(
        task_id="bigeye__ads_derived__ads_client_operations_and_errors_hourly__v1",
        table_id="moz-fx-data-shared-prod.ads_derived.ads_client_operations_and_errors_hourly_v1",
        warehouse_id="1939",
        owner="ahanot@mozilla.com",
        email=[
            "ahanot@mozilla.com",
            "cbeck@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
    )

    bigeye__ads_derived__ads_client_operations_and_errors_hourly__v1.set_upstream(
        ads_derived__ads_client_operations_and_errors_hourly__v1
    )
