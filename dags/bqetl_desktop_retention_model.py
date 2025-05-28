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
### bqetl_desktop_retention_model

Built from bigquery-etl repo, [`dags/bqetl_desktop_retention_model.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_desktop_retention_model.py)

#### Description

Loads the desktop retention model tables
#### Owner

mhirose@mozilla.com

#### Tags

* impact/tier_2
* repo/bigquery-etl
"""


default_args = {
    "owner": "mhirose@mozilla.com",
    "start_date": datetime.datetime(2024, 5, 14, 0, 0),
    "end_date": None,
    "email": ["mhirose@mozilla.com", "telemetry-alerts@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "max_active_tis_per_dag": None,
}

tags = ["impact/tier_2", "repo/bigquery-etl"]

with DAG(
    "bqetl_desktop_retention_model",
    default_args=default_args,
    schedule_interval="0 12 * * *",
    doc_md=docs,
    tags=tags,
    catchup=False,
) as dag:

    wait_for_bigeye__firefox_desktop_derived__baseline_clients_daily__v1 = ExternalTaskSensor(
        task_id="wait_for_bigeye__firefox_desktop_derived__baseline_clients_daily__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="firefox_desktop.bigeye__firefox_desktop_derived__baseline_clients_daily__v1",
        execution_delta=datetime.timedelta(seconds=36000),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_bigeye__firefox_desktop_derived__baseline_clients_first_seen__v1 = ExternalTaskSensor(
        task_id="wait_for_bigeye__firefox_desktop_derived__baseline_clients_first_seen__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="firefox_desktop.bigeye__firefox_desktop_derived__baseline_clients_first_seen__v1",
        execution_delta=datetime.timedelta(seconds=36000),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_bigeye__firefox_desktop_derived__baseline_clients_last_seen__v1 = ExternalTaskSensor(
        task_id="wait_for_bigeye__firefox_desktop_derived__baseline_clients_last_seen__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="firefox_desktop.bigeye__firefox_desktop_derived__baseline_clients_last_seen__v1",
        execution_delta=datetime.timedelta(seconds=36000),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_bigeye__firefox_desktop_derived__desktop_dau_distribution_id_history__v1 = ExternalTaskSensor(
        task_id="wait_for_bigeye__firefox_desktop_derived__desktop_dau_distribution_id_history__v1",
        external_dag_id="bqetl_analytics_tables",
        external_task_id="bigeye__firefox_desktop_derived__desktop_dau_distribution_id_history__v1",
        execution_delta=datetime.timedelta(seconds=36000),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_checks__fail_telemetry_derived__clients_first_seen__v2 = (
        ExternalTaskSensor(
            task_id="wait_for_checks__fail_telemetry_derived__clients_first_seen__v2",
            external_dag_id="bqetl_analytics_tables",
            external_task_id="checks__fail_telemetry_derived__clients_first_seen__v2",
            execution_delta=datetime.timedelta(seconds=36000),
            check_existence=True,
            mode="reschedule",
            poke_interval=datetime.timedelta(minutes=5),
            allowed_states=ALLOWED_STATES,
            failed_states=FAILED_STATES,
            pool="DATA_ENG_EXTERNALTASKSENSOR",
        )
    )

    wait_for_checks__fail_telemetry_derived__clients_last_seen__v2 = ExternalTaskSensor(
        task_id="wait_for_checks__fail_telemetry_derived__clients_last_seen__v2",
        external_dag_id="bqetl_main_summary",
        external_task_id="checks__fail_telemetry_derived__clients_last_seen__v2",
        execution_delta=datetime.timedelta(seconds=36000),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_telemetry_derived__clients_daily_joined__v1 = ExternalTaskSensor(
        task_id="wait_for_telemetry_derived__clients_daily_joined__v1",
        external_dag_id="bqetl_main_summary",
        external_task_id="telemetry_derived__clients_daily_joined__v1",
        execution_delta=datetime.timedelta(seconds=36000),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_clients_first_seen_v3 = ExternalTaskSensor(
        task_id="wait_for_clients_first_seen_v3",
        external_dag_id="bqetl_analytics_tables",
        external_task_id="clients_first_seen_v3",
        execution_delta=datetime.timedelta(seconds=36000),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    checks__fail_firefox_desktop_derived__desktop_retention_clients__v1 = bigquery_dq_check(
        task_id="checks__fail_firefox_desktop_derived__desktop_retention_clients__v1",
        source_table="desktop_retention_clients_v1",
        dataset_id="firefox_desktop_derived",
        project_id="moz-fx-data-shared-prod",
        is_dq_check_fail=True,
        owner="kwindau@mozilla.com",
        email=[
            "kwindau@mozilla.com",
            "mhirose@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        depends_on_past=False,
        parameters=["submission_date:DATE:{{ds}}"],
        retries=0,
    )

    firefox_desktop_derived__desktop_retention_aggregates__v1 = bigquery_etl_query(
        task_id="firefox_desktop_derived__desktop_retention_aggregates__v1",
        destination_table='desktop_retention_aggregates_v1${{ macros.ds_format(macros.ds_add(ds, -27), "%Y-%m-%d", "%Y%m%d") }}',
        dataset_id="firefox_desktop_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kwindau@mozilla.com",
        email=[
            "kwindau@mozilla.com",
            "mhirose@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        parameters=["metric_date:DATE:{{macros.ds_add(ds, -27)}}"]
        + ["submission_date:DATE:{{ds}}"],
    )

    firefox_desktop_derived__desktop_retention_clients__v1 = bigquery_etl_query(
        task_id="firefox_desktop_derived__desktop_retention_clients__v1",
        destination_table="desktop_retention_clients_v1",
        dataset_id="firefox_desktop_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kwindau@mozilla.com",
        email=[
            "kwindau@mozilla.com",
            "mhirose@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    telemetry_derived__desktop_retention__v1 = bigquery_etl_query(
        task_id="telemetry_derived__desktop_retention__v1",
        destination_table='desktop_retention_v1${{ macros.ds_format(macros.ds_add(ds, -27), "%Y-%m-%d", "%Y%m%d") }}',
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="mhirose@mozilla.com",
        email=["mhirose@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        parameters=["metric_date:DATE:{{macros.ds_add(ds, -27)}}"]
        + ["submission_date:DATE:{{ds}}"],
    )

    telemetry_derived__desktop_retention_aggregates__v2 = bigquery_etl_query(
        task_id="telemetry_derived__desktop_retention_aggregates__v2",
        destination_table='desktop_retention_aggregates_v2${{ macros.ds_format(macros.ds_add(ds, -27), "%Y-%m-%d", "%Y%m%d") }}',
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="mhirose@mozilla.com",
        email=["mhirose@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        parameters=["metric_date:DATE:{{macros.ds_add(ds, -27)}}"]
        + ["submission_date:DATE:{{ds}}"],
    )

    telemetry_derived__desktop_retention_clients__v1 = bigquery_etl_query(
        task_id="telemetry_derived__desktop_retention_clients__v1",
        destination_table="desktop_retention_clients_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="mhirose@mozilla.com",
        email=["mhirose@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    telemetry_derived__desktop_retention_clients__v2 = bigquery_etl_query(
        task_id="telemetry_derived__desktop_retention_clients__v2",
        destination_table="desktop_retention_clients_v2",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="mhirose@mozilla.com",
        email=["mhirose@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    checks__fail_firefox_desktop_derived__desktop_retention_clients__v1.set_upstream(
        firefox_desktop_derived__desktop_retention_clients__v1
    )

    firefox_desktop_derived__desktop_retention_aggregates__v1.set_upstream(
        checks__fail_firefox_desktop_derived__desktop_retention_clients__v1
    )

    firefox_desktop_derived__desktop_retention_clients__v1.set_upstream(
        wait_for_bigeye__firefox_desktop_derived__baseline_clients_daily__v1
    )

    firefox_desktop_derived__desktop_retention_clients__v1.set_upstream(
        wait_for_bigeye__firefox_desktop_derived__baseline_clients_first_seen__v1
    )

    firefox_desktop_derived__desktop_retention_clients__v1.set_upstream(
        wait_for_bigeye__firefox_desktop_derived__baseline_clients_last_seen__v1
    )

    firefox_desktop_derived__desktop_retention_clients__v1.set_upstream(
        wait_for_bigeye__firefox_desktop_derived__desktop_dau_distribution_id_history__v1
    )

    telemetry_derived__desktop_retention__v1.set_upstream(
        telemetry_derived__desktop_retention_clients__v1
    )

    telemetry_derived__desktop_retention_aggregates__v2.set_upstream(
        telemetry_derived__desktop_retention_clients__v2
    )

    telemetry_derived__desktop_retention_clients__v1.set_upstream(
        wait_for_checks__fail_telemetry_derived__clients_first_seen__v2
    )

    telemetry_derived__desktop_retention_clients__v1.set_upstream(
        wait_for_checks__fail_telemetry_derived__clients_last_seen__v2
    )

    telemetry_derived__desktop_retention_clients__v1.set_upstream(
        wait_for_telemetry_derived__clients_daily_joined__v1
    )

    telemetry_derived__desktop_retention_clients__v2.set_upstream(
        wait_for_checks__fail_telemetry_derived__clients_last_seen__v2
    )

    telemetry_derived__desktop_retention_clients__v2.set_upstream(
        wait_for_clients_first_seen_v3
    )

    telemetry_derived__desktop_retention_clients__v2.set_upstream(
        wait_for_telemetry_derived__clients_daily_joined__v1
    )
