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
### bqetl_kpis_shredder

Built from bigquery-etl repo, [`dags/bqetl_kpis_shredder.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_kpis_shredder.py)

#### Description

This DAG calculates KPIs for shredder client_ids

#### Owner

lvargas@mozilla.com

#### Tags

* impact/tier_3
* repo/bigquery-etl
"""


default_args = {
    "owner": "lvargas@mozilla.com",
    "start_date": datetime.datetime(2023, 5, 16, 0, 0),
    "end_date": None,
    "email": ["telemetry-alerts@mozilla.com", "lvargas@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
    "max_active_tis_per_dag": None,
}

tags = ["impact/tier_3", "repo/bigquery-etl"]

with DAG(
    "bqetl_kpis_shredder",
    default_args=default_args,
    schedule_interval="0 2 */28 * *",
    doc_md=docs,
    tags=tags,
    catchup=False,
) as dag:

    wait_for_copy_deduplicate_all = ExternalTaskSensor(
        task_id="wait_for_copy_deduplicate_all",
        external_dag_id="copy_deduplicate",
        external_task_id="copy_deduplicate_all",
        execution_delta=datetime.timedelta(seconds=3600),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_fenix_derived__clients_last_seen_joined__v1 = ExternalTaskSensor(
        task_id="wait_for_fenix_derived__clients_last_seen_joined__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="fenix.fenix_derived__clients_last_seen_joined__v1",
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_search_derived__mobile_search_clients_daily__v1 = ExternalTaskSensor(
        task_id="wait_for_search_derived__mobile_search_clients_daily__v1",
        external_dag_id="bqetl_mobile_search",
        external_task_id="search_derived__mobile_search_clients_daily__v1",
        execution_delta=datetime.timedelta(0),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_search_derived__mobile_search_clients_daily__v2 = ExternalTaskSensor(
        task_id="wait_for_search_derived__mobile_search_clients_daily__v2",
        external_dag_id="bqetl_mobile_search",
        external_task_id="search_derived__mobile_search_clients_daily__v2",
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_checks__fail_telemetry_derived__clients_last_seen__v2 = ExternalTaskSensor(
        task_id="wait_for_checks__fail_telemetry_derived__clients_last_seen__v2",
        external_dag_id="bqetl_main_summary",
        external_task_id="checks__fail_telemetry_derived__clients_last_seen__v2",
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_firefox_ios_derived__clients_last_seen_joined__v1 = ExternalTaskSensor(
        task_id="wait_for_firefox_ios_derived__clients_last_seen_joined__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="firefox_ios.firefox_ios_derived__clients_last_seen_joined__v1",
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_focus_ios_derived__clients_last_seen_joined__v1 = ExternalTaskSensor(
        task_id="wait_for_focus_ios_derived__clients_last_seen_joined__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="focus_ios.focus_ios_derived__clients_last_seen_joined__v1",
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_klar_ios_derived__clients_last_seen_joined__v1 = ExternalTaskSensor(
        task_id="wait_for_klar_ios_derived__clients_last_seen_joined__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="klar_ios.klar_ios_derived__clients_last_seen_joined__v1",
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    fenix_active_users_aggregates_for_deletion_requests = bigquery_etl_query(
        task_id="fenix_active_users_aggregates_for_deletion_requests",
        destination_table="active_users_aggregates_deletion_request_v1",
        dataset_id="fenix_derived",
        project_id="moz-fx-data-shared-prod",
        owner="lvargas@mozilla.com",
        email=["lvargas@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="partition_date",
        depends_on_past=False,
        parameters=[
            "end_date:DATE:{{macros.ds_add(ds, 27)}}",
            "start_date:DATE:{{macros.ds_add(ds, 27-28*4)}}",
        ],
    )

    firefox_desktop_active_users_aggregates_for_deletion_requests = bigquery_etl_query(
        task_id="firefox_desktop_active_users_aggregates_for_deletion_requests",
        destination_table="active_users_aggregates_deletion_request_v1",
        dataset_id="firefox_desktop_derived",
        project_id="moz-fx-data-shared-prod",
        owner="lvargas@mozilla.com",
        email=["lvargas@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="partition_date",
        depends_on_past=False,
        parameters=[
            "end_date:DATE:{{macros.ds_add(ds, 27)}}",
            "start_date:DATE:{{macros.ds_add(ds, 27-28*4)}}",
        ],
    )

    firefox_ios_active_users_aggregates_for_deletion_requests = bigquery_etl_query(
        task_id="firefox_ios_active_users_aggregates_for_deletion_requests",
        destination_table="active_users_aggregates_deletion_request_v1",
        dataset_id="firefox_ios_derived",
        project_id="moz-fx-data-shared-prod",
        owner="lvargas@mozilla.com",
        email=["lvargas@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="partition_date",
        depends_on_past=False,
        parameters=[
            "end_date:DATE:{{macros.ds_add(ds, 27)}}",
            "start_date:DATE:{{macros.ds_add(ds, 27-28*4)}}",
        ],
    )

    focus_ios_active_users_aggregates_for_deletion_requests = bigquery_etl_query(
        task_id="focus_ios_active_users_aggregates_for_deletion_requests",
        destination_table="active_users_aggregates_deletion_request_v1",
        dataset_id="focus_ios_derived",
        project_id="moz-fx-data-shared-prod",
        owner="lvargas@mozilla.com",
        email=["lvargas@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="partition_date",
        depends_on_past=False,
        parameters=[
            "end_date:DATE:{{macros.ds_add(ds, 27)}}",
            "start_date:DATE:{{macros.ds_add(ds, 27-28*4)}}",
        ],
    )

    klar_ios_active_users_aggregates_for_deletion_requests = bigquery_etl_query(
        task_id="klar_ios_active_users_aggregates_for_deletion_requests",
        destination_table="active_users_aggregates_deletion_request_v1",
        dataset_id="klar_ios_derived",
        project_id="moz-fx-data-shared-prod",
        owner="lvargas@mozilla.com",
        email=["lvargas@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="partition_date",
        depends_on_past=False,
        parameters=[
            "end_date:DATE:{{macros.ds_add(ds, 27)}}",
            "start_date:DATE:{{macros.ds_add(ds, 27-28*4)}}",
        ],
    )

    fenix_active_users_aggregates_for_deletion_requests.set_upstream(
        wait_for_copy_deduplicate_all
    )

    fenix_active_users_aggregates_for_deletion_requests.set_upstream(
        wait_for_fenix_derived__clients_last_seen_joined__v1
    )

    fenix_active_users_aggregates_for_deletion_requests.set_upstream(
        wait_for_search_derived__mobile_search_clients_daily__v1
    )

    fenix_active_users_aggregates_for_deletion_requests.set_upstream(
        wait_for_search_derived__mobile_search_clients_daily__v2
    )

    firefox_desktop_active_users_aggregates_for_deletion_requests.set_upstream(
        wait_for_checks__fail_telemetry_derived__clients_last_seen__v2
    )

    firefox_desktop_active_users_aggregates_for_deletion_requests.set_upstream(
        wait_for_copy_deduplicate_all
    )

    firefox_desktop_active_users_aggregates_for_deletion_requests.set_upstream(
        wait_for_search_derived__mobile_search_clients_daily__v1
    )

    firefox_ios_active_users_aggregates_for_deletion_requests.set_upstream(
        wait_for_copy_deduplicate_all
    )

    firefox_ios_active_users_aggregates_for_deletion_requests.set_upstream(
        wait_for_firefox_ios_derived__clients_last_seen_joined__v1
    )

    firefox_ios_active_users_aggregates_for_deletion_requests.set_upstream(
        wait_for_search_derived__mobile_search_clients_daily__v1
    )

    firefox_ios_active_users_aggregates_for_deletion_requests.set_upstream(
        wait_for_search_derived__mobile_search_clients_daily__v2
    )

    focus_ios_active_users_aggregates_for_deletion_requests.set_upstream(
        wait_for_copy_deduplicate_all
    )

    focus_ios_active_users_aggregates_for_deletion_requests.set_upstream(
        wait_for_focus_ios_derived__clients_last_seen_joined__v1
    )

    focus_ios_active_users_aggregates_for_deletion_requests.set_upstream(
        wait_for_search_derived__mobile_search_clients_daily__v1
    )

    focus_ios_active_users_aggregates_for_deletion_requests.set_upstream(
        wait_for_search_derived__mobile_search_clients_daily__v2
    )

    klar_ios_active_users_aggregates_for_deletion_requests.set_upstream(
        wait_for_copy_deduplicate_all
    )

    klar_ios_active_users_aggregates_for_deletion_requests.set_upstream(
        wait_for_klar_ios_derived__clients_last_seen_joined__v1
    )

    klar_ios_active_users_aggregates_for_deletion_requests.set_upstream(
        wait_for_search_derived__mobile_search_clients_daily__v1
    )

    klar_ios_active_users_aggregates_for_deletion_requests.set_upstream(
        wait_for_search_derived__mobile_search_clients_daily__v2
    )
