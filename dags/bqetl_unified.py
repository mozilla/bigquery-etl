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
### bqetl_unified

Built from bigquery-etl repo, [`dags/bqetl_unified.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_unified.py)

#### Description

Schedule queries that unify metrics across all products.

#### Owner

ascholtz@mozilla.com

#### Tags

* impact/tier_1
* repo/bigquery-etl
"""


default_args = {
    "owner": "ascholtz@mozilla.com",
    "start_date": datetime.datetime(2021, 10, 12, 0, 0),
    "end_date": None,
    "email": [
        "telemetry-alerts@mozilla.com",
        "ascholtz@mozilla.com",
        "loines@mozilla.com",
        "lvargas@mozilla.com",
    ],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
    "max_active_tis_per_dag": None,
}

tags = ["impact/tier_1", "repo/bigquery-etl"]

with DAG(
    "bqetl_unified",
    default_args=default_args,
    schedule_interval="0 3 * * *",
    doc_md=docs,
    tags=tags,
    catchup=False,
) as dag:

    wait_for_checks__fail_telemetry_derived__clients_last_seen__v2 = ExternalTaskSensor(
        task_id="wait_for_checks__fail_telemetry_derived__clients_last_seen__v2",
        external_dag_id="bqetl_main_summary",
        external_task_id="checks__fail_telemetry_derived__clients_last_seen__v2",
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
        execution_delta=datetime.timedelta(seconds=3600),
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
        execution_delta=datetime.timedelta(seconds=3600),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_focus_android_derived__clients_last_seen_joined__v1 = ExternalTaskSensor(
        task_id="wait_for_focus_android_derived__clients_last_seen_joined__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="focus_android.focus_android_derived__clients_last_seen_joined__v1",
        execution_delta=datetime.timedelta(seconds=3600),
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
        execution_delta=datetime.timedelta(seconds=3600),
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
        execution_delta=datetime.timedelta(seconds=3600),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_telemetry_derived__core_clients_last_seen__v1 = ExternalTaskSensor(
        task_id="wait_for_telemetry_derived__core_clients_last_seen__v1",
        external_dag_id="bqetl_core",
        external_task_id="telemetry_derived__core_clients_last_seen__v1",
        execution_delta=datetime.timedelta(seconds=3600),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    checks__fail_telemetry_derived__unified_metrics__v1 = bigquery_dq_check(
        task_id="checks__fail_telemetry_derived__unified_metrics__v1",
        source_table="unified_metrics_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        is_dq_check_fail=True,
        owner="loines@mozilla.com",
        email=[
            "ascholtz@mozilla.com",
            "loines@mozilla.com",
            "lvargas@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        depends_on_past=False,
        parameters=["submission_date:DATE:{{ds}}"],
        retries=0,
    )

    with TaskGroup(
        "checks__fail_telemetry_derived__unified_metrics__v1_external",
    ) as checks__fail_telemetry_derived__unified_metrics__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_analytics_aggregations__wait_for_checks__fail_telemetry_derived__unified_metrics__v1",
            external_dag_id="bqetl_analytics_aggregations",
            external_task_id="wait_for_checks__fail_telemetry_derived__unified_metrics__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=81900)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_ctxsvc_derived__wait_for_checks__fail_telemetry_derived__unified_metrics__v1",
            external_dag_id="bqetl_ctxsvc_derived",
            external_task_id="wait_for_checks__fail_telemetry_derived__unified_metrics__v1",
        )

        checks__fail_telemetry_derived__unified_metrics__v1_external.set_upstream(
            checks__fail_telemetry_derived__unified_metrics__v1
        )

    checks__warn_telemetry_derived__unified_metrics__v1 = bigquery_dq_check(
        task_id="checks__warn_telemetry_derived__unified_metrics__v1",
        source_table="unified_metrics_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        is_dq_check_fail=False,
        owner="loines@mozilla.com",
        email=[
            "ascholtz@mozilla.com",
            "loines@mozilla.com",
            "lvargas@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        depends_on_past=False,
        parameters=["submission_date:DATE:{{ds}}"],
        retries=0,
    )

    telemetry_derived__rolling_cohorts__v1 = bigquery_etl_query(
        task_id="telemetry_derived__rolling_cohorts__v1",
        destination_table="rolling_cohorts_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="mhirose@mozilla.com",
        email=[
            "ascholtz@mozilla.com",
            "loines@mozilla.com",
            "lvargas@mozilla.com",
            "mhirose@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter="cohort_date",
        depends_on_past=False,
    )

    with TaskGroup(
        "telemetry_derived__rolling_cohorts__v1_external",
    ) as telemetry_derived__rolling_cohorts__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_analytics_aggregations__wait_for_telemetry_derived__rolling_cohorts__v1",
            external_dag_id="bqetl_analytics_aggregations",
            external_task_id="wait_for_telemetry_derived__rolling_cohorts__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=81900)).isoformat() }}",
        )

        telemetry_derived__rolling_cohorts__v1_external.set_upstream(
            telemetry_derived__rolling_cohorts__v1
        )

    telemetry_derived__unified_metrics__v1 = bigquery_etl_query(
        task_id="telemetry_derived__unified_metrics__v1",
        destination_table="unified_metrics_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="loines@mozilla.com",
        email=[
            "ascholtz@mozilla.com",
            "loines@mozilla.com",
            "lvargas@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    checks__fail_telemetry_derived__unified_metrics__v1.set_upstream(
        telemetry_derived__unified_metrics__v1
    )

    checks__warn_telemetry_derived__unified_metrics__v1.set_upstream(
        telemetry_derived__unified_metrics__v1
    )

    telemetry_derived__rolling_cohorts__v1.set_upstream(
        checks__fail_telemetry_derived__unified_metrics__v1
    )

    telemetry_derived__unified_metrics__v1.set_upstream(
        wait_for_checks__fail_telemetry_derived__clients_last_seen__v2
    )

    telemetry_derived__unified_metrics__v1.set_upstream(
        wait_for_fenix_derived__clients_last_seen_joined__v1
    )

    telemetry_derived__unified_metrics__v1.set_upstream(
        wait_for_firefox_ios_derived__clients_last_seen_joined__v1
    )

    telemetry_derived__unified_metrics__v1.set_upstream(
        wait_for_focus_android_derived__clients_last_seen_joined__v1
    )

    telemetry_derived__unified_metrics__v1.set_upstream(
        wait_for_focus_ios_derived__clients_last_seen_joined__v1
    )

    telemetry_derived__unified_metrics__v1.set_upstream(
        wait_for_klar_ios_derived__clients_last_seen_joined__v1
    )

    telemetry_derived__unified_metrics__v1.set_upstream(
        wait_for_telemetry_derived__core_clients_last_seen__v1
    )
