# Generated via https://github.com/mozilla/bigquery-etl/blob/main/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from operators.task_sensor import ExternalTaskCompletedSensor
import datetime
from utils.gcp import bigquery_etl_query, gke_command

docs = """
### bqetl_mozilla_vpn_site_metrics

Built from bigquery-etl repo, [`dags/bqetl_mozilla_vpn_site_metrics.py`](https://github.com/mozilla/bigquery-etl/blob/main/dags/bqetl_mozilla_vpn_site_metrics.py)

#### Description

Daily extracts from the Google Analytics tables for Mozilla VPN as well as
derived tables based on that data.

Depends on Google Analytics exports, which have highly variable timing, so
queries depend on site_metrics_empty_check_v1, which retries every 30
minutes to wait for data to be available.

#### Owner

dthorn@mozilla.com
"""


default_args = {
    "owner": "dthorn@mozilla.com",
    "start_date": datetime.datetime(2021, 4, 22, 0, 0),
    "end_date": None,
    "email": ["telemetry-alerts@mozilla.com", "dthorn@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
}

with DAG(
    "bqetl_mozilla_vpn_site_metrics",
    default_args=default_args,
    schedule_interval="0 15 * * *",
    doc_md=docs,
) as dag:

    mozilla_vpn_derived__funnel_ga_to_subscriptions__v1 = bigquery_etl_query(
        task_id="mozilla_vpn_derived__funnel_ga_to_subscriptions__v1",
        destination_table="funnel_ga_to_subscriptions_v1",
        dataset_id="mozilla_vpn_derived",
        project_id="moz-fx-data-shared-prod",
        owner="dthorn@mozilla.com",
        email=["dthorn@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        dag=dag,
    )

    mozilla_vpn_derived__site_metrics_empty_check__v1 = bigquery_etl_query(
        task_id="mozilla_vpn_derived__site_metrics_empty_check__v1",
        destination_table=None,
        dataset_id="mozilla_vpn_derived",
        project_id="moz-fx-data-shared-prod",
        owner="dthorn@mozilla.com",
        email=["dthorn@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        parameters=["date:DATE:{{ds}}"],
        sql_file_path="sql/moz-fx-data-shared-prod/mozilla_vpn_derived/site_metrics_empty_check_v1/query.sql",
        retry_delay=datetime.timedelta(seconds=1800),
        retries=18,
        email_on_retry=False,
        dag=dag,
    )

    mozilla_vpn_derived__site_metrics_summary__v1 = bigquery_etl_query(
        task_id="mozilla_vpn_derived__site_metrics_summary__v1",
        destination_table="site_metrics_summary_v1",
        dataset_id="mozilla_vpn_derived",
        project_id="moz-fx-data-shared-prod",
        owner="dthorn@mozilla.com",
        email=["dthorn@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="date",
        depends_on_past=False,
        dag=dag,
    )

    wait_for_mozilla_vpn_derived__all_subscriptions__v1 = ExternalTaskCompletedSensor(
        task_id="wait_for_mozilla_vpn_derived__all_subscriptions__v1",
        external_dag_id="bqetl_subplat",
        external_task_id="mozilla_vpn_derived__all_subscriptions__v1",
        execution_delta=datetime.timedelta(seconds=47700),
        check_existence=True,
        mode="reschedule",
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    mozilla_vpn_derived__funnel_ga_to_subscriptions__v1.set_upstream(
        wait_for_mozilla_vpn_derived__all_subscriptions__v1
    )

    mozilla_vpn_derived__funnel_ga_to_subscriptions__v1.set_upstream(
        mozilla_vpn_derived__site_metrics_summary__v1
    )

    mozilla_vpn_derived__site_metrics_summary__v1.set_upstream(
        mozilla_vpn_derived__site_metrics_empty_check__v1
    )
