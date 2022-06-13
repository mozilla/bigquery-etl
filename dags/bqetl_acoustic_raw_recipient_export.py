# Generated via https://github.com/mozilla/bigquery-etl/blob/main/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from operators.task_sensor import ExternalTaskCompletedSensor
import datetime
from utils.gcp import bigquery_etl_query, gke_command

docs = """
### bqetl_acoustic_raw_recipient_export

Built from bigquery-etl repo, [`dags/bqetl_acoustic_raw_recipient_export.py`](https://github.com/mozilla/bigquery-etl/blob/main/dags/bqetl_acoustic_raw_recipient_export.py)

#### Description

Processing data loaded by
fivetran_acoustic_raw_recipient_export
DAG to clean up the data loaded from Acoustic.

#### Owner

kignasiak@mozilla.com
"""


default_args = {
    "owner": "kignasiak@mozilla.com",
    "start_date": datetime.datetime(2022, 3, 1, 0, 0),
    "end_date": None,
    "email": ["telemetry-alerts@mozilla.com", "kignasiak@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=300),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
}

tags = ["impact/tier_3", "repo/bigquery-etl"]

with DAG(
    "bqetl_acoustic_raw_recipient_export",
    default_args=default_args,
    schedule_interval="0 9 * * *",
    doc_md=docs,
    tags=tags,
) as dag:

    acoustic__raw_recipient__v1 = bigquery_etl_query(
        task_id="acoustic__raw_recipient__v1",
        destination_table="raw_recipient_v1",
        dataset_id="acoustic",
        project_id="moz-fx-data-marketing-prod",
        owner="kignasiak@mozilla.com",
        email=["kignasiak@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    acoustic__raw_recipient_raw__v1 = bigquery_etl_query(
        task_id="acoustic__raw_recipient_raw__v1",
        destination_table="raw_recipient_raw_v1",
        dataset_id="acoustic",
        project_id="moz-fx-data-marketing-prod",
        owner="kignasiak@mozilla.com",
        email=["kignasiak@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    acoustic__raw_recipient__v1.set_upstream(acoustic__raw_recipient_raw__v1)

    wait_for_fivetran_load_completed = ExternalTaskCompletedSensor(
        task_id="wait_for_fivetran_load_completed",
        external_dag_id="fivetran_acoustic_raw_recipient_export",
        external_task_id="fivetran_load_completed",
        execution_delta=datetime.timedelta(seconds=3600),
        check_existence=True,
        mode="reschedule",
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    acoustic__raw_recipient_raw__v1.set_upstream(wait_for_fivetran_load_completed)
