# Generated via https://github.com/mozilla/bigquery-etl/blob/main/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from operators.task_sensor import ExternalTaskCompletedSensor
import datetime
from utils.gcp import bigquery_etl_query, gke_command

docs = """
### bqetl_event_rollup

Built from bigquery-etl repo, [`dags/bqetl_event_rollup.py`](https://github.com/mozilla/bigquery-etl/blob/main/dags/bqetl_event_rollup.py)

#### Owner

wlachance@mozilla.com
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
}

with DAG(
    "bqetl_event_rollup",
    default_args=default_args,
    schedule_interval="0 3 * * *",
    doc_md=docs,
) as dag:

    firefox_accounts_derived__event_types__v1 = bigquery_etl_query(
        task_id="firefox_accounts_derived__event_types__v1",
        destination_table="event_types_v1",
        dataset_id="firefox_accounts_derived",
        project_id="moz-fx-data-shared-prod",
        owner="wlachance@mozilla.com",
        email=["akomar@mozilla.com", "wlachance@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        parameters=["submission_date:DATE:{{ds}}"],
        dag=dag,
    )

    firefox_accounts_derived__event_types_history__v1 = bigquery_etl_query(
        task_id="firefox_accounts_derived__event_types_history__v1",
        destination_table="event_types_history_v1",
        dataset_id="firefox_accounts_derived",
        project_id="moz-fx-data-shared-prod",
        owner="wlachance@mozilla.com",
        email=["akomar@mozilla.com", "wlachance@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=True,
        dag=dag,
    )

    firefox_accounts_derived__events_daily__v1 = bigquery_etl_query(
        task_id="firefox_accounts_derived__events_daily__v1",
        destination_table="events_daily_v1",
        dataset_id="firefox_accounts_derived",
        project_id="moz-fx-data-shared-prod",
        owner="wlachance@mozilla.com",
        email=["akomar@mozilla.com", "wlachance@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        dag=dag,
    )

    funnel_events_source__v1 = bigquery_etl_query(
        task_id="funnel_events_source__v1",
        destination_table="funnel_events_source_v1",
        dataset_id="firefox_accounts_derived",
        project_id="moz-fx-data-shared-prod",
        owner="wlachance@mozilla.com",
        email=["wlachance@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        dag=dag,
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
        parameters=["submission_date:DATE:{{ds}}"],
        dag=dag,
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
        dag=dag,
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
        dag=dag,
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
        parameters=["submission_date:DATE:{{ds}}"],
        dag=dag,
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
        dag=dag,
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
        dag=dag,
    )

    telemetry_derived__event_types__v1 = bigquery_etl_query(
        task_id="telemetry_derived__event_types__v1",
        destination_table="event_types_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="wlachance@mozilla.com",
        email=["akomar@mozilla.com", "wlachance@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        parameters=["submission_date:DATE:{{ds}}"],
        dag=dag,
    )

    telemetry_derived__event_types_history__v1 = bigquery_etl_query(
        task_id="telemetry_derived__event_types_history__v1",
        destination_table="event_types_history_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="wlachance@mozilla.com",
        email=["akomar@mozilla.com", "wlachance@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=True,
        dag=dag,
    )

    telemetry_derived__events_daily__v1 = bigquery_etl_query(
        task_id="telemetry_derived__events_daily__v1",
        destination_table="events_daily_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="wlachance@mozilla.com",
        email=["akomar@mozilla.com", "wlachance@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        dag=dag,
    )

    firefox_accounts_derived__event_types__v1.set_upstream(
        firefox_accounts_derived__event_types_history__v1
    )

    firefox_accounts_derived__event_types_history__v1.set_upstream(
        funnel_events_source__v1
    )

    firefox_accounts_derived__events_daily__v1.set_upstream(
        firefox_accounts_derived__event_types__v1
    )

    wait_for_firefox_accounts_derived__fxa_auth_events__v1 = (
        ExternalTaskCompletedSensor(
            task_id="wait_for_firefox_accounts_derived__fxa_auth_events__v1",
            external_dag_id="bqetl_fxa_events",
            external_task_id="firefox_accounts_derived__fxa_auth_events__v1",
            execution_delta=datetime.timedelta(seconds=5400),
            check_existence=True,
            mode="reschedule",
            pool="DATA_ENG_EXTERNALTASKSENSOR",
        )
    )

    funnel_events_source__v1.set_upstream(
        wait_for_firefox_accounts_derived__fxa_auth_events__v1
    )
    wait_for_firefox_accounts_derived__fxa_content_events__v1 = (
        ExternalTaskCompletedSensor(
            task_id="wait_for_firefox_accounts_derived__fxa_content_events__v1",
            external_dag_id="bqetl_fxa_events",
            external_task_id="firefox_accounts_derived__fxa_content_events__v1",
            execution_delta=datetime.timedelta(seconds=5400),
            check_existence=True,
            mode="reschedule",
            pool="DATA_ENG_EXTERNALTASKSENSOR",
        )
    )

    funnel_events_source__v1.set_upstream(
        wait_for_firefox_accounts_derived__fxa_content_events__v1
    )

    messaging_system_derived__event_types__v1.set_upstream(
        messaging_system_derived__event_types_history__v1
    )

    wait_for_copy_deduplicate_all = ExternalTaskCompletedSensor(
        task_id="wait_for_copy_deduplicate_all",
        external_dag_id="copy_deduplicate",
        external_task_id="copy_deduplicate_all",
        execution_delta=datetime.timedelta(seconds=7200),
        check_existence=True,
        mode="reschedule",
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    messaging_system_derived__event_types_history__v1.set_upstream(
        wait_for_copy_deduplicate_all
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

    mozilla_vpn_derived__events_daily__v1.set_upstream(
        mozilla_vpn_derived__event_types__v1
    )

    telemetry_derived__event_types__v1.set_upstream(
        telemetry_derived__event_types_history__v1
    )

    telemetry_derived__event_types_history__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    telemetry_derived__events_daily__v1.set_upstream(telemetry_derived__event_types__v1)
