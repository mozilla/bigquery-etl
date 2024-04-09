# Generated via https://github.com/mozilla/bigquery-etl/blob/main/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskMarker
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
import datetime
from operators.gcp_container_operator import GKEPodOperator
from utils.constants import ALLOWED_STATES, FAILED_STATES
from utils.gcp import bigquery_etl_query, bigquery_dq_check

docs = """
### bqetl_data_observability_test_data_copy

Built from bigquery-etl repo, [`dags/bqetl_data_observability_test_data_copy.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_data_observability_test_data_copy.py)

#### Description

ETL used to copy over data to data-observability-dev BQ project used
for testing out different data observability platforms.

#### Owner

kik@mozilla.com

#### Tags

* impact/tier_3
* repo/bigquery-etl
* triage/no_triage
"""


default_args = {
    "owner": "kik@mozilla.com",
    "start_date": datetime.datetime(2024, 3, 20, 0, 0),
    "end_date": None,
    "email": ["kik@mozilla.com", "akommasani@mozilla.com", "ascholtz@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
}

tags = ["impact/tier_3", "repo/bigquery-etl", "triage/no_triage"]

with DAG(
    "bqetl_data_observability_test_data_copy",
    default_args=default_args,
    schedule_interval="0 8 * * *",
    doc_md=docs,
    tags=tags,
) as dag:

    wait_for_fenix_derived__clients_last_seen_joined__v1 = ExternalTaskSensor(
        task_id="wait_for_fenix_derived__clients_last_seen_joined__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="fenix.fenix_derived__clients_last_seen_joined__v1",
        execution_delta=datetime.timedelta(seconds=21600),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_fenix_derived__event_types__v1 = ExternalTaskSensor(
        task_id="wait_for_fenix_derived__event_types__v1",
        external_dag_id="bqetl_fenix_event_rollup",
        external_task_id="fenix_derived__event_types__v1",
        execution_delta=datetime.timedelta(seconds=21600),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_fenix_derived__events_daily__v1 = ExternalTaskSensor(
        task_id="wait_for_fenix_derived__events_daily__v1",
        external_dag_id="bqetl_fenix_event_rollup",
        external_task_id="fenix_derived__events_daily__v1",
        execution_delta=datetime.timedelta(seconds=21600),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_checks__fail_fenix_derived__firefox_android_clients__v1 = (
        ExternalTaskSensor(
            task_id="wait_for_checks__fail_fenix_derived__firefox_android_clients__v1",
            external_dag_id="bqetl_analytics_tables",
            external_task_id="checks__fail_fenix_derived__firefox_android_clients__v1",
            execution_delta=datetime.timedelta(seconds=21600),
            check_existence=True,
            mode="reschedule",
            allowed_states=ALLOWED_STATES,
            failed_states=FAILED_STATES,
            pool="DATA_ENG_EXTERNALTASKSENSOR",
        )
    )

    wait_for_fenix_derived__metrics_clients_last_seen__v1 = ExternalTaskSensor(
        task_id="wait_for_fenix_derived__metrics_clients_last_seen__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="fenix.fenix_derived__metrics_clients_last_seen__v1",
        execution_delta=datetime.timedelta(seconds=21600),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    fenix_derived__clients_last_seen_joined__v1 = bigquery_etl_query(
        task_id="fenix_derived__clients_last_seen_joined__v1",
        destination_table='clients_last_seen_joined_v1${{ macros.ds_format(macros.ds_add(ds, -1), "%Y-%m-%d", "%Y%m%d") }}',
        dataset_id="fenix_derived",
        project_id="data-observability-dev",
        owner="kik@mozilla.com",
        email=["akommasani@mozilla.com", "ascholtz@mozilla.com", "kik@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=True,
        parameters=["submission_date:DATE:{{macros.ds_add(ds, -1)}}"],
    )

    fenix_derived__event_types__v1 = bigquery_etl_query(
        task_id="fenix_derived__event_types__v1",
        destination_table="event_types_v1",
        dataset_id="fenix_derived",
        project_id="data-observability-dev",
        owner="kik@mozilla.com",
        email=["akommasani@mozilla.com", "ascholtz@mozilla.com", "kik@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
        parameters=["submission_date:DATE:{{ds}}"],
    )

    fenix_derived__events_daily__v1 = bigquery_etl_query(
        task_id="fenix_derived__events_daily__v1",
        destination_table="events_daily_v1",
        dataset_id="fenix_derived",
        project_id="data-observability-dev",
        owner="kik@mozilla.com",
        email=["akommasani@mozilla.com", "ascholtz@mozilla.com", "kik@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    fenix_derived__firefox_android_clients__v1 = bigquery_etl_query(
        task_id="fenix_derived__firefox_android_clients__v1",
        destination_table="firefox_android_clients_v1",
        dataset_id="fenix_derived",
        project_id="data-observability-dev",
        owner="kik@mozilla.com",
        email=["akommasani@mozilla.com", "ascholtz@mozilla.com", "kik@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=True,
        parameters=["submission_date:DATE:{{ds}}"],
    )

    fenix_derived__metrics_clients_last_seen__v1 = bigquery_etl_query(
        task_id="fenix_derived__metrics_clients_last_seen__v1",
        destination_table="metrics_clients_last_seen_v1",
        dataset_id="fenix_derived",
        project_id="data-observability-dev",
        owner="kik@mozilla.com",
        email=["akommasani@mozilla.com", "ascholtz@mozilla.com", "kik@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    fenix_derived__clients_last_seen_joined__v1.set_upstream(
        wait_for_fenix_derived__clients_last_seen_joined__v1
    )

    fenix_derived__event_types__v1.set_upstream(wait_for_fenix_derived__event_types__v1)

    fenix_derived__events_daily__v1.set_upstream(
        wait_for_fenix_derived__events_daily__v1
    )

    fenix_derived__firefox_android_clients__v1.set_upstream(
        wait_for_checks__fail_fenix_derived__firefox_android_clients__v1
    )

    fenix_derived__metrics_clients_last_seen__v1.set_upstream(
        wait_for_fenix_derived__metrics_clients_last_seen__v1
    )
