# Generated via https://github.com/mozilla/bigquery-etl/blob/main/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskMarker
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
import datetime
from utils.constants import ALLOWED_STATES, FAILED_STATES
from utils.gcp import bigquery_etl_query, gke_command, bigquery_dq_check

from fivetran_provider.operators.fivetran import FivetranOperator
from fivetran_provider.sensors.fivetran import FivetranSensor
from utils.callbacks import retry_tasks_callback

docs = """
### bqetl_monitoring_airflow

Built from bigquery-etl repo, [`dags/bqetl_monitoring_airflow.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_monitoring_airflow.py)

#### Description

This DAG schedules queries and scripts for populating datasets
used for monitoring of Airflow DAGs.

#### Owner

kik@mozilla.com

#### Tags

* impact/tier_2
* repo/bigquery-etl
"""


default_args = {
    "owner": "kik@mozilla.com",
    "start_date": datetime.datetime(2022, 9, 1, 0, 0),
    "end_date": None,
    "email": ["kik@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
}

tags = ["impact/tier_2", "repo/bigquery-etl"]

with DAG(
    "bqetl_monitoring_airflow",
    default_args=default_args,
    schedule_interval="0 10 * * *",
    doc_md=docs,
    tags=tags,
) as dag:
    monitoring_derived__airflow_dag__v1 = bigquery_etl_query(
        task_id="monitoring_derived__airflow_dag__v1",
        destination_table="airflow_dag_v1",
        dataset_id="monitoring_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    monitoring_derived__airflow_dag_note__v1 = bigquery_etl_query(
        task_id="monitoring_derived__airflow_dag_note__v1",
        destination_table="airflow_dag_note_v1",
        dataset_id="monitoring_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    monitoring_derived__airflow_dag_owner_attributes__v1 = bigquery_etl_query(
        task_id="monitoring_derived__airflow_dag_owner_attributes__v1",
        destination_table="airflow_dag_owner_attributes_v1",
        dataset_id="monitoring_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    monitoring_derived__airflow_dag_run__v1 = bigquery_etl_query(
        task_id="monitoring_derived__airflow_dag_run__v1",
        destination_table="airflow_dag_run_v1",
        dataset_id="monitoring_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    monitoring_derived__airflow_dag_tag__v1 = bigquery_etl_query(
        task_id="monitoring_derived__airflow_dag_tag__v1",
        destination_table="airflow_dag_tag_v1",
        dataset_id="monitoring_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    monitoring_derived__airflow_dag_warning__v1 = bigquery_etl_query(
        task_id="monitoring_derived__airflow_dag_warning__v1",
        destination_table="airflow_dag_warning_v1",
        dataset_id="monitoring_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    monitoring_derived__airflow_import_error__v1 = bigquery_etl_query(
        task_id="monitoring_derived__airflow_import_error__v1",
        destination_table="airflow_import_error_v1",
        dataset_id="monitoring_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    monitoring_derived__airflow_job__v1 = bigquery_etl_query(
        task_id="monitoring_derived__airflow_job__v1",
        destination_table="airflow_job_v1",
        dataset_id="monitoring_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    monitoring_derived__airflow_slot_pool__v1 = bigquery_etl_query(
        task_id="monitoring_derived__airflow_slot_pool__v1",
        destination_table="airflow_slot_pool_v1",
        dataset_id="monitoring_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    monitoring_derived__airflow_task_fail__v1 = bigquery_etl_query(
        task_id="monitoring_derived__airflow_task_fail__v1",
        destination_table="airflow_task_fail_v1",
        dataset_id="monitoring_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    monitoring_derived__airflow_task_instance__v1 = bigquery_etl_query(
        task_id="monitoring_derived__airflow_task_instance__v1",
        destination_table="airflow_task_instance_v1",
        dataset_id="monitoring_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    monitoring_derived__airflow_task_instance_note__v1 = bigquery_etl_query(
        task_id="monitoring_derived__airflow_task_instance_note__v1",
        destination_table="airflow_task_instance_note_v1",
        dataset_id="monitoring_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    monitoring_derived__airflow_task_reschedule__v1 = bigquery_etl_query(
        task_id="monitoring_derived__airflow_task_reschedule__v1",
        destination_table="airflow_task_reschedule_v1",
        dataset_id="monitoring_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    monitoring_derived__airflow_trigger__v1 = bigquery_etl_query(
        task_id="monitoring_derived__airflow_trigger__v1",
        destination_table="airflow_trigger_v1",
        dataset_id="monitoring_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    monitoring_derived__airflow_user__v1 = bigquery_etl_query(
        task_id="monitoring_derived__airflow_user__v1",
        destination_table="airflow_user_v1",
        dataset_id="monitoring_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    fivetran_airflow_metadata_import_sync_start = FivetranOperator(
        connector_id="{{ var.value.fivetran_airflow_metadata_import_connector_id }}",
        task_id="fivetran_airflow_metadata_import_task",
    )

    fivetran_airflow_metadata_import_sync_wait = FivetranSensor(
        connector_id="{{ var.value.fivetran_airflow_metadata_import_connector_id }}",
        task_id="fivetran_airflow_metadata_import_sensor",
        poke_interval=30,
        xcom="{{ task_instance.xcom_pull('fivetran_airflow_metadata_import_task') }}",
        on_retry_callback=retry_tasks_callback,
        params={"retry_tasks": ["fivetran_airflow_metadata_import_task"]},
    )

    fivetran_airflow_metadata_import_sync_wait.set_upstream(
        fivetran_airflow_metadata_import_sync_start
    )

    monitoring_derived__airflow_dag__v1.set_upstream(
        fivetran_airflow_metadata_import_sync_wait
    )

    monitoring_derived__airflow_dag_note__v1.set_upstream(
        fivetran_airflow_metadata_import_sync_wait
    )

    monitoring_derived__airflow_dag_owner_attributes__v1.set_upstream(
        fivetran_airflow_metadata_import_sync_wait
    )

    monitoring_derived__airflow_dag_run__v1.set_upstream(
        fivetran_airflow_metadata_import_sync_wait
    )

    monitoring_derived__airflow_dag_tag__v1.set_upstream(
        fivetran_airflow_metadata_import_sync_wait
    )

    monitoring_derived__airflow_dag_warning__v1.set_upstream(
        fivetran_airflow_metadata_import_sync_wait
    )

    monitoring_derived__airflow_import_error__v1.set_upstream(
        fivetran_airflow_metadata_import_sync_wait
    )

    monitoring_derived__airflow_job__v1.set_upstream(
        fivetran_airflow_metadata_import_sync_wait
    )

    monitoring_derived__airflow_slot_pool__v1.set_upstream(
        fivetran_airflow_metadata_import_sync_wait
    )

    monitoring_derived__airflow_task_fail__v1.set_upstream(
        fivetran_airflow_metadata_import_sync_wait
    )

    monitoring_derived__airflow_task_instance__v1.set_upstream(
        fivetran_airflow_metadata_import_sync_wait
    )

    monitoring_derived__airflow_task_instance_note__v1.set_upstream(
        fivetran_airflow_metadata_import_sync_wait
    )

    monitoring_derived__airflow_task_reschedule__v1.set_upstream(
        fivetran_airflow_metadata_import_sync_wait
    )

    monitoring_derived__airflow_trigger__v1.set_upstream(
        fivetran_airflow_metadata_import_sync_wait
    )

    monitoring_derived__airflow_user__v1.set_upstream(
        fivetran_airflow_metadata_import_sync_wait
    )
