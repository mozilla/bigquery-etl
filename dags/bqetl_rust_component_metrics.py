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
### bqetl_rust_component_metrics

Built from bigquery-etl repo, [`dags/bqetl_rust_component_metrics.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_rust_component_metrics.py)

#### Description

The DAG schedules rust component metric queries.
#### Owner

sync-team@mozilla.com

#### Tags

* impact/tier_3
* repo/bigquery-etl
"""


default_args = {
    "owner": "sync-team@mozilla.com",
    "start_date": datetime.datetime(2026, 2, 5, 0, 0),
    "end_date": None,
    "email": ["telemetry-alerts@mozilla.com", "sync-team@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
    "max_active_tis_per_dag": None,
}

tags = ["impact/tier_3", "repo/bigquery-etl"]

with DAG(
    "bqetl_rust_component_metrics",
    default_args=default_args,
    schedule_interval="0 3 * * *",
    doc_md=docs,
    tags=tags,
    catchup=False,
) as dag:

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

    rust_components_derived__db_size_after_maintenance__v1 = bigquery_etl_query(
        task_id="rust_components_derived__db_size_after_maintenance__v1",
        destination_table="db_size_after_maintenance_v1",
        dataset_id="rust_components_derived",
        project_id="moz-fx-data-shared-prod",
        owner="sync-team@mozilla.com",
        email=["sync-team@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    rust_components_derived__ingest_download_time__v1 = bigquery_etl_query(
        task_id="rust_components_derived__ingest_download_time__v1",
        destination_table="ingest_download_time_v1",
        dataset_id="rust_components_derived",
        project_id="moz-fx-data-shared-prod",
        owner="sync-team@mozilla.com",
        email=["sync-team@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    rust_components_derived__ingest_time__v1 = bigquery_etl_query(
        task_id="rust_components_derived__ingest_time__v1",
        destination_table="ingest_time_v1",
        dataset_id="rust_components_derived",
        project_id="moz-fx-data-shared-prod",
        owner="sync-team@mozilla.com",
        email=["sync-team@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    rust_components_derived__key_regenerated_corrupt__v1 = bigquery_etl_query(
        task_id="rust_components_derived__key_regenerated_corrupt__v1",
        destination_table="key_regenerated_corrupt_v1",
        dataset_id="rust_components_derived",
        project_id="moz-fx-data-shared-prod",
        owner="sync-team@mozilla.com",
        email=["sync-team@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    rust_components_derived__key_regenerated_lost__v1 = bigquery_etl_query(
        task_id="rust_components_derived__key_regenerated_lost__v1",
        destination_table="key_regenerated_lost_v1",
        dataset_id="rust_components_derived",
        project_id="moz-fx-data-shared-prod",
        owner="sync-team@mozilla.com",
        email=["sync-team@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    rust_components_derived__key_regenerated_other__v1 = bigquery_etl_query(
        task_id="rust_components_derived__key_regenerated_other__v1",
        destination_table="key_regenerated_other_v1",
        dataset_id="rust_components_derived",
        project_id="moz-fx-data-shared-prod",
        owner="sync-team@mozilla.com",
        email=["sync-team@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    rust_components_derived__mirror_undecryptable_deleted__v1 = bigquery_etl_query(
        task_id="rust_components_derived__mirror_undecryptable_deleted__v1",
        destination_table="mirror_undecryptable_deleted_v1",
        dataset_id="rust_components_derived",
        project_id="moz-fx-data-shared-prod",
        owner="sync-team@mozilla.com",
        email=["sync-team@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    rust_components_derived__query_time__v1 = bigquery_etl_query(
        task_id="rust_components_derived__query_time__v1",
        destination_table="query_time_v1",
        dataset_id="rust_components_derived",
        project_id="moz-fx-data-shared-prod",
        owner="sync-team@mozilla.com",
        email=["sync-team@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    rust_components_derived__run_maintenance_chk_pnt_time__v1 = bigquery_etl_query(
        task_id="rust_components_derived__run_maintenance_chk_pnt_time__v1",
        destination_table="run_maintenance_chk_pnt_time_v1",
        dataset_id="rust_components_derived",
        project_id="moz-fx-data-shared-prod",
        owner="sync-team@mozilla.com",
        email=["sync-team@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    rust_components_derived__run_maintenance_optimize_time__v1 = bigquery_etl_query(
        task_id="rust_components_derived__run_maintenance_optimize_time__v1",
        destination_table="run_maintenance_optimize_time_v1",
        dataset_id="rust_components_derived",
        project_id="moz-fx-data-shared-prod",
        owner="sync-team@mozilla.com",
        email=["sync-team@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    rust_components_derived__run_maintenance_prune_time__v1 = bigquery_etl_query(
        task_id="rust_components_derived__run_maintenance_prune_time__v1",
        destination_table="run_maintenance_prune_time_v1",
        dataset_id="rust_components_derived",
        project_id="moz-fx-data-shared-prod",
        owner="sync-team@mozilla.com",
        email=["sync-team@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    rust_components_derived__run_maintenance_time__v1 = bigquery_etl_query(
        task_id="rust_components_derived__run_maintenance_time__v1",
        destination_table="run_maintenance_time_v1",
        dataset_id="rust_components_derived",
        project_id="moz-fx-data-shared-prod",
        owner="sync-team@mozilla.com",
        email=["sync-team@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    rust_components_derived__run_maintenance_vacuum_time__v1 = bigquery_etl_query(
        task_id="rust_components_derived__run_maintenance_vacuum_time__v1",
        destination_table="run_maintenance_vacuum_time_v1",
        dataset_id="rust_components_derived",
        project_id="moz-fx-data-shared-prod",
        owner="sync-team@mozilla.com",
        email=["sync-team@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    rust_components_derived__db_size_after_maintenance__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    rust_components_derived__ingest_download_time__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    rust_components_derived__ingest_time__v1.set_upstream(wait_for_copy_deduplicate_all)

    rust_components_derived__key_regenerated_corrupt__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    rust_components_derived__key_regenerated_lost__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    rust_components_derived__key_regenerated_other__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    rust_components_derived__mirror_undecryptable_deleted__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    rust_components_derived__query_time__v1.set_upstream(wait_for_copy_deduplicate_all)

    rust_components_derived__run_maintenance_chk_pnt_time__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    rust_components_derived__run_maintenance_optimize_time__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    rust_components_derived__run_maintenance_prune_time__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    rust_components_derived__run_maintenance_time__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    rust_components_derived__run_maintenance_vacuum_time__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )
