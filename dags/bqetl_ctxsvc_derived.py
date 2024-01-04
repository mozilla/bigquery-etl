# Generated via https://github.com/mozilla/bigquery-etl/blob/main/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskMarker
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
import datetime
from utils.constants import ALLOWED_STATES, FAILED_STATES
from utils.gcp import bigquery_etl_query, gke_command, bigquery_dq_check

docs = """
### bqetl_ctxsvc_derived

Built from bigquery-etl repo, [`dags/bqetl_ctxsvc_derived.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_ctxsvc_derived.py)

#### Description

Contextual services derived tables
#### Owner

ctroy@mozilla.com

#### Tags

* impact/tier_2
* repo/bigquery-etl
"""


default_args = {
    "owner": "ctroy@mozilla.com",
    "start_date": datetime.datetime(2021, 5, 1, 0, 0),
    "end_date": None,
    "email": [
        "ctroy@mozilla.com",
        "wstuckey@mozilla.com",
        "telemetry-alerts@mozilla.com",
    ],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
}

tags = ["impact/tier_2", "repo/bigquery-etl"]

with DAG(
    "bqetl_ctxsvc_derived",
    default_args=default_args,
    schedule_interval="0 3 * * *",
    doc_md=docs,
    tags=tags,
) as dag:
    contextual_services_derived__adm_forecasting__v1 = bigquery_etl_query(
        task_id="contextual_services_derived__adm_forecasting__v1",
        destination_table="adm_forecasting_v1",
        dataset_id="contextual_services_derived",
        project_id="moz-fx-data-shared-prod",
        owner="skahmann@mozilla.com",
        email=[
            "ctroy@mozilla.com",
            "skahmann@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wstuckey@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    contextual_services_derived__event_aggregates__v1 = bigquery_etl_query(
        task_id="contextual_services_derived__event_aggregates__v1",
        destination_table="event_aggregates_v1",
        dataset_id="contextual_services_derived",
        project_id="moz-fx-data-shared-prod",
        owner="rburwei@mozilla.com",
        email=[
            "ctroy@mozilla.com",
            "rburwei@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wstuckey@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        arguments=["--schema_update_option=ALLOW_FIELD_ADDITION"],
    )

    contextual_services_derived__event_aggregates_check__v1 = bigquery_etl_query(
        task_id="contextual_services_derived__event_aggregates_check__v1",
        destination_table=None,
        dataset_id="contextual_services_derived",
        project_id="moz-fx-data-shared-prod",
        owner="wstuckey@mozilla.com",
        email=[
            "ctroy@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wstuckey@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        parameters=["submission_date:DATE:{{ds}}"],
        sql_file_path="sql/moz-fx-data-shared-prod/contextual_services_derived/event_aggregates_check_v1/query.sql",
    )

    contextual_services_derived__event_aggregates_spons_tiles__v1 = bigquery_etl_query(
        task_id="contextual_services_derived__event_aggregates_spons_tiles__v1",
        destination_table="event_aggregates_spons_tiles_v1",
        dataset_id="contextual_services_derived",
        project_id="moz-fx-data-shared-prod",
        owner="rburwei@mozilla.com",
        email=[
            "ctroy@mozilla.com",
            "rburwei@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wstuckey@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    contextual_services_derived__event_aggregates_suggest__v1 = bigquery_etl_query(
        task_id="contextual_services_derived__event_aggregates_suggest__v1",
        destination_table="event_aggregates_suggest_v1",
        dataset_id="contextual_services_derived",
        project_id="moz-fx-data-shared-prod",
        owner="rburwei@mozilla.com",
        email=[
            "ctroy@mozilla.com",
            "rburwei@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wstuckey@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    contextual_services_derived__request_payload_suggest__v2 = bigquery_etl_query(
        task_id="contextual_services_derived__request_payload_suggest__v2",
        destination_table="request_payload_suggest_v2",
        dataset_id="contextual_services_derived",
        project_id="moz-fx-data-shared-prod",
        owner="skahmann@mozilla.com",
        email=[
            "akommasani@mozilla.com",
            "ctroy@mozilla.com",
            "skahmann@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wstuckey@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    contextual_services_derived__request_payload_tiles__v2 = bigquery_etl_query(
        task_id="contextual_services_derived__request_payload_tiles__v2",
        destination_table="request_payload_tiles_v2",
        dataset_id="contextual_services_derived",
        project_id="moz-fx-data-shared-prod",
        owner="skahmann@mozilla.com",
        email=[
            "akommasani@mozilla.com",
            "ctroy@mozilla.com",
            "skahmann@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wstuckey@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    contextual_services_derived__suggest_revenue_levers_daily__v1 = bigquery_etl_query(
        task_id="contextual_services_derived__suggest_revenue_levers_daily__v1",
        destination_table="suggest_revenue_levers_daily_v1",
        dataset_id="contextual_services_derived",
        project_id="moz-fx-data-shared-prod",
        owner="skahmann@mozilla.com",
        email=[
            "ctroy@mozilla.com",
            "skahmann@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wstuckey@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    wait_for_checks__fail_telemetry_derived__unified_metrics__v1 = ExternalTaskSensor(
        task_id="wait_for_checks__fail_telemetry_derived__unified_metrics__v1",
        external_dag_id="bqetl_unified",
        external_task_id="checks__fail_telemetry_derived__unified_metrics__v1",
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    contextual_services_derived__adm_forecasting__v1.set_upstream(
        wait_for_checks__fail_telemetry_derived__unified_metrics__v1
    )

    contextual_services_derived__adm_forecasting__v1.set_upstream(
        contextual_services_derived__event_aggregates__v1
    )
    wait_for_telemetry_derived__clients_daily_joined__v1 = ExternalTaskSensor(
        task_id="wait_for_telemetry_derived__clients_daily_joined__v1",
        external_dag_id="bqetl_main_summary",
        external_task_id="telemetry_derived__clients_daily_joined__v1",
        execution_delta=datetime.timedelta(seconds=3600),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    contextual_services_derived__adm_forecasting__v1.set_upstream(
        wait_for_telemetry_derived__clients_daily_joined__v1
    )

    wait_for_copy_deduplicate_all = ExternalTaskSensor(
        task_id="wait_for_copy_deduplicate_all",
        external_dag_id="copy_deduplicate",
        external_task_id="copy_deduplicate_all",
        execution_delta=datetime.timedelta(seconds=7200),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    contextual_services_derived__event_aggregates__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    contextual_services_derived__event_aggregates_check__v1.set_upstream(
        contextual_services_derived__event_aggregates__v1
    )

    contextual_services_derived__event_aggregates_spons_tiles__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    contextual_services_derived__event_aggregates_suggest__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    contextual_services_derived__request_payload_suggest__v2.set_upstream(
        wait_for_copy_deduplicate_all
    )

    contextual_services_derived__suggest_revenue_levers_daily__v1.set_upstream(
        wait_for_checks__fail_telemetry_derived__unified_metrics__v1
    )
    wait_for_search_derived__search_clients_daily__v8 = ExternalTaskSensor(
        task_id="wait_for_search_derived__search_clients_daily__v8",
        external_dag_id="bqetl_search",
        external_task_id="search_derived__search_clients_daily__v8",
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    contextual_services_derived__suggest_revenue_levers_daily__v1.set_upstream(
        wait_for_search_derived__search_clients_daily__v8
    )
    wait_for_telemetry_derived__suggest_clients_daily__v1 = ExternalTaskSensor(
        task_id="wait_for_telemetry_derived__suggest_clients_daily__v1",
        external_dag_id="bqetl_main_summary",
        external_task_id="telemetry_derived__suggest_clients_daily__v1",
        execution_delta=datetime.timedelta(seconds=3600),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    contextual_services_derived__suggest_revenue_levers_daily__v1.set_upstream(
        wait_for_telemetry_derived__suggest_clients_daily__v1
    )
