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
### bqetl_search_dashboard

Built from bigquery-etl repo, [`dags/bqetl_search_dashboard.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_search_dashboard.py)

#### Owner

akomar@mozilla.com

#### Tags

* impact/tier_2
* repo/bigquery-etl
"""


default_args = {
    "owner": "akomar@mozilla.com",
    "start_date": datetime.datetime(2020, 12, 14, 0, 0),
    "end_date": None,
    "email": ["telemetry-alerts@mozilla.com", "akomar@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
}

tags = ["impact/tier_2", "repo/bigquery-etl"]

with DAG(
    "bqetl_search_dashboard",
    default_args=default_args,
    schedule_interval="30 4 * * *",
    doc_md=docs,
    tags=tags,
) as dag:

    wait_for_checks__fail_telemetry_derived__clients_last_seen__v2 = ExternalTaskSensor(
        task_id="wait_for_checks__fail_telemetry_derived__clients_last_seen__v2",
        external_dag_id="bqetl_main_summary",
        external_task_id="checks__fail_telemetry_derived__clients_last_seen__v2",
        execution_delta=datetime.timedelta(seconds=9000),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_search_derived__search_aggregates__v8 = ExternalTaskSensor(
        task_id="wait_for_search_derived__search_aggregates__v8",
        external_dag_id="bqetl_search",
        external_task_id="search_derived__search_aggregates__v8",
        execution_delta=datetime.timedelta(seconds=5400),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_search_derived__mobile_search_clients_daily__v1 = ExternalTaskSensor(
        task_id="wait_for_search_derived__mobile_search_clients_daily__v1",
        external_dag_id="bqetl_mobile_search",
        external_task_id="search_derived__mobile_search_clients_daily__v1",
        execution_delta=datetime.timedelta(seconds=9000),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_checks__fail_fenix_derived__active_users_aggregates__v3 = (
        ExternalTaskSensor(
            task_id="wait_for_checks__fail_fenix_derived__active_users_aggregates__v3",
            external_dag_id="bqetl_analytics_aggregations",
            external_task_id="checks__fail_fenix_derived__active_users_aggregates__v3",
            execution_delta=datetime.timedelta(seconds=900),
            check_existence=True,
            mode="reschedule",
            allowed_states=ALLOWED_STATES,
            failed_states=FAILED_STATES,
            pool="DATA_ENG_EXTERNALTASKSENSOR",
        )
    )

    wait_for_checks__fail_firefox_ios_derived__active_users_aggregates__v3 = ExternalTaskSensor(
        task_id="wait_for_checks__fail_firefox_ios_derived__active_users_aggregates__v3",
        external_dag_id="bqetl_analytics_aggregations",
        external_task_id="checks__fail_firefox_ios_derived__active_users_aggregates__v3",
        execution_delta=datetime.timedelta(seconds=900),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_checks__fail_focus_android_derived__active_users_aggregates__v3 = ExternalTaskSensor(
        task_id="wait_for_checks__fail_focus_android_derived__active_users_aggregates__v3",
        external_dag_id="bqetl_analytics_aggregations",
        external_task_id="checks__fail_focus_android_derived__active_users_aggregates__v3",
        execution_delta=datetime.timedelta(seconds=900),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_checks__fail_focus_ios_derived__active_users_aggregates__v3 = ExternalTaskSensor(
        task_id="wait_for_checks__fail_focus_ios_derived__active_users_aggregates__v3",
        external_dag_id="bqetl_analytics_aggregations",
        external_task_id="checks__fail_focus_ios_derived__active_users_aggregates__v3",
        execution_delta=datetime.timedelta(seconds=900),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_checks__fail_klar_android_derived__active_users_aggregates__v3 = ExternalTaskSensor(
        task_id="wait_for_checks__fail_klar_android_derived__active_users_aggregates__v3",
        external_dag_id="bqetl_analytics_aggregations",
        external_task_id="checks__fail_klar_android_derived__active_users_aggregates__v3",
        execution_delta=datetime.timedelta(seconds=900),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_checks__fail_klar_ios_derived__active_users_aggregates__v3 = ExternalTaskSensor(
        task_id="wait_for_checks__fail_klar_ios_derived__active_users_aggregates__v3",
        external_dag_id="bqetl_analytics_aggregations",
        external_task_id="checks__fail_klar_ios_derived__active_users_aggregates__v3",
        execution_delta=datetime.timedelta(seconds=900),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_firefox_desktop_active_users_aggregates = ExternalTaskSensor(
        task_id="wait_for_firefox_desktop_active_users_aggregates",
        external_dag_id="bqetl_analytics_aggregations",
        external_task_id="firefox_desktop_active_users_aggregates",
        execution_delta=datetime.timedelta(seconds=900),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_search_derived__search_clients_daily__v8 = ExternalTaskSensor(
        task_id="wait_for_search_derived__search_clients_daily__v8",
        external_dag_id="bqetl_search",
        external_task_id="search_derived__search_clients_daily__v8",
        execution_delta=datetime.timedelta(seconds=5400),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    search_derived__desktop_search_aggregates_by_userstate__v1 = bigquery_etl_query(
        task_id="search_derived__desktop_search_aggregates_by_userstate__v1",
        destination_table="desktop_search_aggregates_by_userstate_v1",
        dataset_id="search_derived",
        project_id="moz-fx-data-shared-prod",
        owner="cmorales@mozilla.com",
        email=[
            "akomar@mozilla.com",
            "cmorales@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "xluo@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    search_derived__desktop_search_aggregates_for_searchreport__v1 = bigquery_etl_query(
        task_id="search_derived__desktop_search_aggregates_for_searchreport__v1",
        destination_table="desktop_search_aggregates_for_searchreport_v1",
        dataset_id="search_derived",
        project_id="moz-fx-data-shared-prod",
        owner="cmorales@mozilla.com",
        email=[
            "akomar@mozilla.com",
            "cmorales@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "xluo@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    search_derived__mobile_search_aggregates_for_searchreport__v1 = bigquery_etl_query(
        task_id="search_derived__mobile_search_aggregates_for_searchreport__v1",
        destination_table="mobile_search_aggregates_for_searchreport_v1",
        dataset_id="search_derived",
        project_id="moz-fx-data-shared-prod",
        owner="cmorales@mozilla.com",
        email=[
            "akomar@mozilla.com",
            "cmorales@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "xluo@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    search_derived__search_revenue_levers_daily__v1 = bigquery_etl_query(
        task_id="search_derived__search_revenue_levers_daily__v1",
        destination_table='search_revenue_levers_daily_v1${{ macros.ds_format(macros.ds_add(ds, -1), "%Y-%m-%d", "%Y%m%d") }}',
        dataset_id="search_derived",
        project_id="moz-fx-data-shared-prod",
        owner="xluo@mozilla.com",
        email=[
            "akomar@mozilla.com",
            "cmorales@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "xluo@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        parameters=["submission_date:DATE:{{macros.ds_add(ds, -1)}}"],
    )

    search_derived__desktop_search_aggregates_by_userstate__v1.set_upstream(
        wait_for_checks__fail_telemetry_derived__clients_last_seen__v2
    )

    search_derived__desktop_search_aggregates_for_searchreport__v1.set_upstream(
        wait_for_search_derived__search_aggregates__v8
    )

    search_derived__mobile_search_aggregates_for_searchreport__v1.set_upstream(
        wait_for_search_derived__mobile_search_clients_daily__v1
    )

    search_derived__search_revenue_levers_daily__v1.set_upstream(
        wait_for_checks__fail_fenix_derived__active_users_aggregates__v3
    )

    search_derived__search_revenue_levers_daily__v1.set_upstream(
        wait_for_checks__fail_firefox_ios_derived__active_users_aggregates__v3
    )

    search_derived__search_revenue_levers_daily__v1.set_upstream(
        wait_for_checks__fail_focus_android_derived__active_users_aggregates__v3
    )

    search_derived__search_revenue_levers_daily__v1.set_upstream(
        wait_for_checks__fail_focus_ios_derived__active_users_aggregates__v3
    )

    search_derived__search_revenue_levers_daily__v1.set_upstream(
        wait_for_checks__fail_klar_android_derived__active_users_aggregates__v3
    )

    search_derived__search_revenue_levers_daily__v1.set_upstream(
        wait_for_checks__fail_klar_ios_derived__active_users_aggregates__v3
    )

    search_derived__search_revenue_levers_daily__v1.set_upstream(
        wait_for_firefox_desktop_active_users_aggregates
    )

    search_derived__search_revenue_levers_daily__v1.set_upstream(
        wait_for_search_derived__mobile_search_clients_daily__v1
    )

    search_derived__search_revenue_levers_daily__v1.set_upstream(
        wait_for_search_derived__search_clients_daily__v8
    )