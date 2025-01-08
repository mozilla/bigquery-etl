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
### bqetl_fx_cert_error_privacy_dashboard

Built from bigquery-etl repo, [`dags/bqetl_fx_cert_error_privacy_dashboard.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_fx_cert_error_privacy_dashboard.py)

#### Description

This DAG builds the desktop event_aggregates table for unique users count

#### Owner

akommasani@mozilla.com

#### Tags

* impact/tier_3
* repo/bigquery-etl
"""


default_args = {
    "owner": "akommasani@mozilla.com",
    "start_date": datetime.datetime(2024, 12, 19, 0, 0),
    "end_date": None,
    "email": ["telemetry-alerts@mozilla.com", "akommasani@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=300),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
}

tags = ["impact/tier_3", "repo/bigquery-etl"]

with DAG(
    "bqetl_fx_cert_error_privacy_dashboard",
    default_args=default_args,
    schedule_interval="40 16 * * *",
    doc_md=docs,
    tags=tags,
) as dag:

    wait_for_bq_main_events = ExternalTaskSensor(
        task_id="wait_for_bq_main_events",
        external_dag_id="copy_deduplicate",
        external_task_id="bq_main_events",
        execution_delta=datetime.timedelta(seconds=56400),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_event_events = ExternalTaskSensor(
        task_id="wait_for_event_events",
        external_dag_id="copy_deduplicate",
        external_task_id="event_events",
        execution_delta=datetime.timedelta(seconds=56400),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    telemetry_derived__fx_cert_error_unique_users_normalized_channel__v1 = bigquery_etl_query(
        task_id="telemetry_derived__fx_cert_error_unique_users_normalized_channel__v1",
        destination_table="fx_cert_error_unique_users_normalized_channel_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="akommasani@mozilla.com",
        email=["akommasani@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    telemetry_derived__fx_cert_error_unique_users_os__v1 = bigquery_etl_query(
        task_id="telemetry_derived__fx_cert_error_unique_users_os__v1",
        destination_table="fx_cert_error_unique_users_os_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="akommasani@mozilla.com",
        email=["akommasani@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    telemetry_derived__fx_privacy_dau_agg__v1 = bigquery_etl_query(
        task_id="telemetry_derived__fx_privacy_dau_agg__v1",
        destination_table="fx_privacy_dau_agg_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="akommasani@mozilla.com",
        email=["akommasani@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    telemetry_derived__fx_cert_error_unique_users_normalized_channel__v1.set_upstream(
        wait_for_bq_main_events
    )

    telemetry_derived__fx_cert_error_unique_users_normalized_channel__v1.set_upstream(
        wait_for_event_events
    )

    telemetry_derived__fx_cert_error_unique_users_os__v1.set_upstream(
        wait_for_bq_main_events
    )

    telemetry_derived__fx_cert_error_unique_users_os__v1.set_upstream(
        wait_for_event_events
    )

    telemetry_derived__fx_privacy_dau_agg__v1.set_upstream(wait_for_bq_main_events)

    telemetry_derived__fx_privacy_dau_agg__v1.set_upstream(wait_for_event_events)
