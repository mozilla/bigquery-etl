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
### bqetl_dynamic_dau

Built from bigquery-etl repo, [`dags/bqetl_dynamic_dau.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_dynamic_dau.py)

#### Description

Calculates rolling 28 day DAU for different populations

#### Owner

kwindau@mozilla.com

#### Tags

* impact/tier_2
* repo/bigquery-etl
"""


default_args = {
    "owner": "kwindau@mozilla.com",
    "start_date": datetime.datetime(2024, 9, 26, 0, 0),
    "end_date": None,
    "email": ["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
}

tags = ["impact/tier_2", "repo/bigquery-etl"]

with DAG(
    "bqetl_dynamic_dau",
    default_args=default_args,
    schedule_interval="0 14 * * *",
    doc_md=docs,
    tags=tags,
) as dag:

    wait_for_checks__fail_fenix_derived__active_users_aggregates__v3 = (
        ExternalTaskSensor(
            task_id="wait_for_checks__fail_fenix_derived__active_users_aggregates__v3",
            external_dag_id="bqetl_analytics_aggregations",
            external_task_id="checks__fail_fenix_derived__active_users_aggregates__v3",
            execution_delta=datetime.timedelta(seconds=35100),
            check_existence=True,
            mode="reschedule",
            poke_interval=datetime.timedelta(minutes=5),
            allowed_states=ALLOWED_STATES,
            failed_states=FAILED_STATES,
            pool="DATA_ENG_EXTERNALTASKSENSOR",
        )
    )

    wait_for_checks__fail_firefox_ios_derived__active_users_aggregates__v3 = ExternalTaskSensor(
        task_id="wait_for_checks__fail_firefox_ios_derived__active_users_aggregates__v3",
        external_dag_id="bqetl_analytics_aggregations",
        external_task_id="checks__fail_firefox_ios_derived__active_users_aggregates__v3",
        execution_delta=datetime.timedelta(seconds=35100),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_checks__fail_focus_android_derived__active_users_aggregates__v3 = ExternalTaskSensor(
        task_id="wait_for_checks__fail_focus_android_derived__active_users_aggregates__v3",
        external_dag_id="bqetl_analytics_aggregations",
        external_task_id="checks__fail_focus_android_derived__active_users_aggregates__v3",
        execution_delta=datetime.timedelta(seconds=35100),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_checks__fail_focus_ios_derived__active_users_aggregates__v3 = ExternalTaskSensor(
        task_id="wait_for_checks__fail_focus_ios_derived__active_users_aggregates__v3",
        external_dag_id="bqetl_analytics_aggregations",
        external_task_id="checks__fail_focus_ios_derived__active_users_aggregates__v3",
        execution_delta=datetime.timedelta(seconds=35100),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_checks__fail_klar_android_derived__active_users_aggregates__v3 = ExternalTaskSensor(
        task_id="wait_for_checks__fail_klar_android_derived__active_users_aggregates__v3",
        external_dag_id="bqetl_analytics_aggregations",
        external_task_id="checks__fail_klar_android_derived__active_users_aggregates__v3",
        execution_delta=datetime.timedelta(seconds=35100),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_checks__fail_klar_ios_derived__active_users_aggregates__v3 = ExternalTaskSensor(
        task_id="wait_for_checks__fail_klar_ios_derived__active_users_aggregates__v3",
        external_dag_id="bqetl_analytics_aggregations",
        external_task_id="checks__fail_klar_ios_derived__active_users_aggregates__v3",
        execution_delta=datetime.timedelta(seconds=35100),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_firefox_desktop_active_users_aggregates_v4 = ExternalTaskSensor(
        task_id="wait_for_firefox_desktop_active_users_aggregates_v4",
        external_dag_id="bqetl_analytics_aggregations",
        external_task_id="firefox_desktop_active_users_aggregates_v4",
        execution_delta=datetime.timedelta(seconds=35100),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    checks__warn_telemetry_derived__segmented_dau_28_day_rolling__v1 = (
        bigquery_dq_check(
            task_id="checks__warn_telemetry_derived__segmented_dau_28_day_rolling__v1",
            source_table="segmented_dau_28_day_rolling_v1",
            dataset_id="telemetry_derived",
            project_id="moz-fx-data-shared-prod",
            is_dq_check_fail=False,
            owner="kwindau@mozilla.com",
            email=["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
            depends_on_past=False,
            parameters=["submission_date:DATE:{{ds}}"],
            retries=0,
        )
    )

    telemetry_derived__segmented_dau_28_day_rolling__v1 = bigquery_etl_query(
        task_id="telemetry_derived__segmented_dau_28_day_rolling__v1",
        destination_table='segmented_dau_28_day_rolling_v1${{ macros.ds_format(macros.ds_add(ds, -1), "%Y-%m-%d", "%Y%m%d") }}',
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        parameters=["submission_date:DATE:{{macros.ds_add(ds, -1)}}"],
    )

    checks__warn_telemetry_derived__segmented_dau_28_day_rolling__v1.set_upstream(
        telemetry_derived__segmented_dau_28_day_rolling__v1
    )

    telemetry_derived__segmented_dau_28_day_rolling__v1.set_upstream(
        wait_for_checks__fail_fenix_derived__active_users_aggregates__v3
    )

    telemetry_derived__segmented_dau_28_day_rolling__v1.set_upstream(
        wait_for_checks__fail_firefox_ios_derived__active_users_aggregates__v3
    )

    telemetry_derived__segmented_dau_28_day_rolling__v1.set_upstream(
        wait_for_checks__fail_focus_android_derived__active_users_aggregates__v3
    )

    telemetry_derived__segmented_dau_28_day_rolling__v1.set_upstream(
        wait_for_checks__fail_focus_ios_derived__active_users_aggregates__v3
    )

    telemetry_derived__segmented_dau_28_day_rolling__v1.set_upstream(
        wait_for_checks__fail_klar_android_derived__active_users_aggregates__v3
    )

    telemetry_derived__segmented_dau_28_day_rolling__v1.set_upstream(
        wait_for_checks__fail_klar_ios_derived__active_users_aggregates__v3
    )

    telemetry_derived__segmented_dau_28_day_rolling__v1.set_upstream(
        wait_for_firefox_desktop_active_users_aggregates_v4
    )
