# Generated via https://github.com/mozilla/bigquery-etl/blob/main/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskMarker
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
import datetime
from utils.constants import ALLOWED_STATES, FAILED_STATES
from utils.gcp import bigquery_etl_query, gke_command, bigquery_dq_check

docs = """
### bqetl_fivetran_copied_tables

Built from bigquery-etl repo, [`dags/bqetl_fivetran_copied_tables.py`](https://github.com/mozilla/bigquery-etl/blob/main/dags/bqetl_fivetran_copied_tables.py)

#### Description

Copy over Fivetran data to shared-prod.

#### Owner

frank@mozilla.com
"""


default_args = {
    "owner": "frank@mozilla.com",
    "start_date": datetime.datetime(2023, 7, 4, 0, 0),
    "end_date": None,
    "email": ["telemetry-alerts@mozilla.com", "frank@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
}

tags = ["impact/tier_2", "repo/bigquery-etl"]

with DAG(
    "bqetl_fivetran_copied_tables",
    default_args=default_args,
    schedule_interval="0 3 * * *",
    doc_md=docs,
    tags=tags,
) as dag:
    app_store_external__firefox_app_store_territory_source_type_report__v1 = bigquery_etl_query(
        task_id="app_store_external__firefox_app_store_territory_source_type_report__v1",
        destination_table="firefox_app_store_territory_source_type_report_v1",
        dataset_id="app_store_external",
        project_id="moz-fx-data-shared-prod",
        owner="frank@mozilla.com",
        email=["frank@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    with TaskGroup(
        "app_store_external__firefox_app_store_territory_source_type_report__v1_external",
    ) as app_store_external__firefox_app_store_territory_source_type_report__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_firefox_ios__wait_for_app_store_external__firefox_app_store_territory_source_type_report__v1",
            external_dag_id="bqetl_firefox_ios",
            external_task_id="wait_for_app_store_external__firefox_app_store_territory_source_type_report__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=82800)).isoformat() }}",
        )

        app_store_external__firefox_app_store_territory_source_type_report__v1_external.set_upstream(
            app_store_external__firefox_app_store_territory_source_type_report__v1
        )

    app_store_external__firefox_downloads_territory_source_type_report__v1 = bigquery_etl_query(
        task_id="app_store_external__firefox_downloads_territory_source_type_report__v1",
        destination_table="firefox_downloads_territory_source_type_report_v1",
        dataset_id="app_store_external",
        project_id="moz-fx-data-shared-prod",
        owner="frank@mozilla.com",
        email=["frank@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    with TaskGroup(
        "app_store_external__firefox_downloads_territory_source_type_report__v1_external",
    ) as app_store_external__firefox_downloads_territory_source_type_report__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_firefox_ios__wait_for_app_store_external__firefox_downloads_territory_source_type_report__v1",
            external_dag_id="bqetl_firefox_ios",
            external_task_id="wait_for_app_store_external__firefox_downloads_territory_source_type_report__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=82800)).isoformat() }}",
        )

        app_store_external__firefox_downloads_territory_source_type_report__v1_external.set_upstream(
            app_store_external__firefox_downloads_territory_source_type_report__v1
        )

    app_store_external__firefox_usage_territory_source_type_report__v1 = bigquery_etl_query(
        task_id="app_store_external__firefox_usage_territory_source_type_report__v1",
        destination_table="firefox_usage_territory_source_type_report_v1",
        dataset_id="app_store_external",
        project_id="moz-fx-data-shared-prod",
        owner="frank@mozilla.com",
        email=["frank@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )
