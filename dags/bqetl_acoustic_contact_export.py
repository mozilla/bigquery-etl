# Generated via https://github.com/mozilla/bigquery-etl/blob/main/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskMarker
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
import datetime
from utils.constants import ALLOWED_STATES, FAILED_STATES
from utils.gcp import bigquery_etl_query, gke_command, bigquery_dq_check

docs = """
### bqetl_acoustic_contact_export

Built from bigquery-etl repo, [`dags/bqetl_acoustic_contact_export.py`](https://github.com/mozilla/bigquery-etl/blob/main/dags/bqetl_acoustic_contact_export.py)

#### Description

Processing data loaded by
fivetran_acoustic_contact_export
DAG to clean up the data loaded from Acoustic.

#### Owner

kik@mozilla.com
"""


default_args = {
    "owner": "kik@mozilla.com",
    "start_date": datetime.datetime(2021, 3, 1, 0, 0),
    "end_date": None,
    "email": ["telemetry-alerts@mozilla.com", "kik@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=300),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
}

tags = ["impact/tier_3", "repo/bigquery-etl"]

with DAG(
    "bqetl_acoustic_contact_export",
    default_args=default_args,
    schedule_interval="0 9 * * *",
    doc_md=docs,
    tags=tags,
) as dag:
    acoustic__contact__v1 = bigquery_etl_query(
        task_id="acoustic__contact__v1",
        destination_table="contact_v1",
        dataset_id="acoustic",
        project_id="moz-fx-data-marketing-prod",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    acoustic__contact_current_snapshot__v1 = bigquery_etl_query(
        task_id="acoustic__contact_current_snapshot__v1",
        destination_table="contact_current_snapshot_v1",
        dataset_id="acoustic",
        project_id="moz-fx-data-marketing-prod",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    acoustic__contact_raw__v1 = bigquery_etl_query(
        task_id="acoustic__contact_raw__v1",
        destination_table="contact_raw_v1",
        dataset_id="acoustic",
        project_id="moz-fx-data-marketing-prod",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    acoustic__contact__v1.set_upstream(acoustic__contact_raw__v1)

    acoustic__contact_current_snapshot__v1.set_upstream(acoustic__contact__v1)

    wait_for_fivetran_load_completed = ExternalTaskSensor(
        task_id="wait_for_fivetran_load_completed",
        external_dag_id="fivetran_acoustic_contact_export",
        external_task_id="fivetran_load_completed",
        execution_delta=datetime.timedelta(seconds=3600),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    acoustic__contact_raw__v1.set_upstream(wait_for_fivetran_load_completed)
