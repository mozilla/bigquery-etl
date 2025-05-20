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
### bqetl_mozilla_vpn_site_metrics

Built from bigquery-etl repo, [`dags/bqetl_mozilla_vpn_site_metrics.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_mozilla_vpn_site_metrics.py)

#### Description

Daily extracts from the Google Analytics tables for Mozilla VPN as well as
derived tables based on that data.

Depends on Google Analytics exports, which have highly variable timing, so
queries depend on site_metrics_empty_check_v1, which retries every 30
minutes to wait for data to be available.

#### Owner

srose@mozilla.com

#### Tags

* impact/tier_2
* repo/bigquery-etl
"""


default_args = {
    "owner": "srose@mozilla.com",
    "start_date": datetime.datetime(2021, 4, 22, 0, 0),
    "end_date": None,
    "email": ["telemetry-alerts@mozilla.com", "srose@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
    "max_active_tis_per_dag": None,
}

tags = ["impact/tier_2", "repo/bigquery-etl"]

with DAG(
    "bqetl_mozilla_vpn_site_metrics",
    default_args=default_args,
    schedule_interval="0 15 * * *",
    doc_md=docs,
    tags=tags,
    catchup=False,
) as dag:

    wait_for_mozilla_vpn_derived__all_subscriptions__v1 = ExternalTaskSensor(
        task_id="wait_for_mozilla_vpn_derived__all_subscriptions__v1",
        external_dag_id="bqetl_subplat",
        external_task_id="mozilla_vpn_derived__all_subscriptions__v1",
        execution_delta=datetime.timedelta(seconds=10800),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_wait_for_wmo_events_table = ExternalTaskSensor(
        task_id="wait_for_wait_for_wmo_events_table",
        external_dag_id="bqetl_google_analytics_derived_ga4",
        external_task_id="wait_for_wmo_events_table",
        execution_delta=datetime.timedelta(seconds=10800),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    mozilla_vpn_derived__funnel_ga_to_subscriptions__v2 = bigquery_etl_query(
        task_id="mozilla_vpn_derived__funnel_ga_to_subscriptions__v2",
        destination_table="funnel_ga_to_subscriptions_v2",
        dataset_id="mozilla_vpn_derived",
        project_id="moz-fx-data-shared-prod",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="date",
        depends_on_past=False,
    )

    mozilla_vpn_derived__site_metrics_summary__v2 = bigquery_etl_query(
        task_id="mozilla_vpn_derived__site_metrics_summary__v2",
        destination_table="site_metrics_summary_v2",
        dataset_id="mozilla_vpn_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kwindau@mozilla.com",
        email=[
            "kwindau@mozilla.com",
            "srose@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    mozilla_vpn_derived__funnel_ga_to_subscriptions__v2.set_upstream(
        wait_for_mozilla_vpn_derived__all_subscriptions__v1
    )

    mozilla_vpn_derived__funnel_ga_to_subscriptions__v2.set_upstream(
        mozilla_vpn_derived__site_metrics_summary__v2
    )

    mozilla_vpn_derived__site_metrics_summary__v2.set_upstream(
        wait_for_wait_for_wmo_events_table
    )
