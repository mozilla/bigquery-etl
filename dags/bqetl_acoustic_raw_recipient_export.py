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
### bqetl_acoustic_raw_recipient_export

Built from bigquery-etl repo, [`dags/bqetl_acoustic_raw_recipient_export.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_acoustic_raw_recipient_export.py)

#### Description

Processing data loaded by
fivetran_acoustic_raw_recipient_export
DAG to clean up the data loaded from Acoustic.

#### Owner

cbeck@mozilla.com

#### Tags

* impact/tier_3
* repo/bigquery-etl
"""


default_args = {
    "owner": "cbeck@mozilla.com",
    "start_date": datetime.datetime(2024, 4, 3, 0, 0),
    "end_date": None,
    "email": ["telemetry-alerts@mozilla.com", "cbeck@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=300),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
    "max_active_tis_per_dag": None,
}

tags = ["impact/tier_3", "repo/bigquery-etl"]

with DAG(
    "bqetl_acoustic_raw_recipient_export",
    default_args=default_args,
    schedule_interval="0 9 * * *",
    doc_md=docs,
    tags=tags,
    catchup=False,
) as dag:

    wait_for_fivetran_load_completed = ExternalTaskSensor(
        task_id="wait_for_fivetran_load_completed",
        external_dag_id="fivetran_acoustic_raw_recipient_export",
        external_task_id="fivetran_load_completed",
        execution_delta=datetime.timedelta(seconds=3600),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    acoustic_derived__raw_recipient__v1 = bigquery_etl_query(
        task_id="acoustic_derived__raw_recipient__v1",
        destination_table="raw_recipient_v1",
        dataset_id="acoustic_derived",
        project_id="moz-fx-data-shared-prod",
        owner="cbeck@mozilla.com",
        email=["cbeck@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    acoustic_external__raw_recipient_raw__v1 = bigquery_etl_query(
        task_id="acoustic_external__raw_recipient_raw__v1",
        destination_table="raw_recipient_raw_v1",
        dataset_id="acoustic_external",
        project_id="moz-fx-data-shared-prod",
        owner="cbeck@mozilla.com",
        email=["cbeck@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    acoustic_derived__raw_recipient__v1.set_upstream(
        acoustic_external__raw_recipient_raw__v1
    )

    acoustic_external__raw_recipient_raw__v1.set_upstream(
        wait_for_fivetran_load_completed
    )
