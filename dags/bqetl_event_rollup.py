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
### bqetl_event_rollup

Built from bigquery-etl repo, [`dags/bqetl_event_rollup.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_event_rollup.py)

#### Description

Desktop tables (`telemetry_derived.events_daily_v1` and upstream) are deprecated and paused
(have their scheduling metadata commented out) per https://bugzilla.mozilla.org/show_bug.cgi?id=1805722#c10

#### Owner

wlachance@mozilla.com

#### Tags

* impact/tier_1
* repo/bigquery-etl
"""


default_args = {
    "owner": "wlachance@mozilla.com",
    "start_date": datetime.datetime(2020, 11, 3, 0, 0),
    "end_date": None,
    "email": ["wlachance@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
    "max_active_tis_per_dag": None,
}

tags = ["impact/tier_1", "repo/bigquery-etl"]

with DAG(
    "bqetl_event_rollup",
    default_args=default_args,
    schedule_interval="0 3 * * *",
    doc_md=docs,
    tags=tags,
    catchup=False,
) as dag:

    wait_for_firefox_desktop_derived__onboarding__v2 = ExternalTaskSensor(
        task_id="wait_for_firefox_desktop_derived__onboarding__v2",
        external_dag_id="bqetl_messaging_system",
        external_task_id="firefox_desktop_derived__onboarding__v2",
        execution_delta=datetime.timedelta(seconds=3600),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_copy_deduplicate_all = ExternalTaskSensor(
        task_id="wait_for_copy_deduplicate_all",
        external_dag_id="copy_deduplicate",
        external_task_id="copy_deduplicate_all",
        execution_delta=datetime.timedelta(seconds=7200),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    messaging_system_derived__event_types__v1 = bigquery_etl_query(
        task_id="messaging_system_derived__event_types__v1",
        destination_table="event_types_v1",
        dataset_id="messaging_system_derived",
        project_id="moz-fx-data-shared-prod",
        owner="wlachance@mozilla.com",
        email=["akomar@mozilla.com", "wlachance@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
        parameters=["submission_date:DATE:{{ds}}"],
    )

    messaging_system_derived__event_types_history__v1 = bigquery_etl_query(
        task_id="messaging_system_derived__event_types_history__v1",
        destination_table="event_types_history_v1",
        dataset_id="messaging_system_derived",
        project_id="moz-fx-data-shared-prod",
        owner="wlachance@mozilla.com",
        email=["akomar@mozilla.com", "wlachance@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=True,
    )

    messaging_system_derived__events_daily__v1 = bigquery_etl_query(
        task_id="messaging_system_derived__events_daily__v1",
        destination_table="events_daily_v1",
        dataset_id="messaging_system_derived",
        project_id="moz-fx-data-shared-prod",
        owner="wlachance@mozilla.com",
        email=["akomar@mozilla.com", "wlachance@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    mozilla_vpn_derived__event_types__v1 = bigquery_etl_query(
        task_id="mozilla_vpn_derived__event_types__v1",
        destination_table="event_types_v1",
        dataset_id="mozilla_vpn_derived",
        project_id="moz-fx-data-shared-prod",
        owner="wlachance@mozilla.com",
        email=["akomar@mozilla.com", "wlachance@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
        parameters=["submission_date:DATE:{{ds}}"],
    )

    mozilla_vpn_derived__event_types_history__v1 = bigquery_etl_query(
        task_id="mozilla_vpn_derived__event_types_history__v1",
        destination_table="event_types_history_v1",
        dataset_id="mozilla_vpn_derived",
        project_id="moz-fx-data-shared-prod",
        owner="wlachance@mozilla.com",
        email=["akomar@mozilla.com", "wlachance@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=True,
    )

    mozilla_vpn_derived__events_daily__v1 = bigquery_etl_query(
        task_id="mozilla_vpn_derived__events_daily__v1",
        destination_table="events_daily_v1",
        dataset_id="mozilla_vpn_derived",
        project_id="moz-fx-data-shared-prod",
        owner="wlachance@mozilla.com",
        email=["akomar@mozilla.com", "wlachance@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    messaging_system_derived__event_types__v1.set_upstream(
        messaging_system_derived__event_types_history__v1
    )

    messaging_system_derived__event_types_history__v1.set_upstream(
        wait_for_firefox_desktop_derived__onboarding__v2
    )

    messaging_system_derived__events_daily__v1.set_upstream(
        wait_for_firefox_desktop_derived__onboarding__v2
    )

    messaging_system_derived__events_daily__v1.set_upstream(
        messaging_system_derived__event_types__v1
    )

    mozilla_vpn_derived__event_types__v1.set_upstream(
        mozilla_vpn_derived__event_types_history__v1
    )

    mozilla_vpn_derived__event_types_history__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    mozilla_vpn_derived__events_daily__v1.set_upstream(wait_for_copy_deduplicate_all)

    mozilla_vpn_derived__events_daily__v1.set_upstream(
        mozilla_vpn_derived__event_types__v1
    )
