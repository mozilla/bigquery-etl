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
### bqetl_acoustic_suppression_list

Built from bigquery-etl repo, [`dags/bqetl_acoustic_suppression_list.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_acoustic_suppression_list.py)

#### Description

ETL for Acoustic suppression list.

#### Owner

leli@mozilla.com

#### Tags

* impact/tier_3
* repo/bigquery-etl
"""


default_args = {
    "owner": "leli@mozilla.com",
    "start_date": datetime.datetime(2024, 3, 20, 0, 0),
    "end_date": None,
    "email": ["leli@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
}

tags = ["impact/tier_3", "repo/bigquery-etl"]

with DAG(
    "bqetl_acoustic_suppression_list",
    default_args=default_args,
    schedule_interval="0 9 * * *",
    doc_md=docs,
    tags=tags,
) as dag:

    wait_for_acoustic__contact_raw__v1 = ExternalTaskSensor(
        task_id="wait_for_acoustic__contact_raw__v1",
        external_dag_id="bqetl_acoustic_contact_export",
        external_task_id="acoustic__contact_raw__v1",
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    acoustic__suppression_list__v1 = bigquery_etl_query(
        task_id="acoustic__suppression_list__v1",
        destination_table="suppression_list_v1",
        dataset_id="acoustic",
        project_id="moz-fx-data-marketing-prod",
        owner="leli@mozilla.com",
        email=["leli@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    acoustic__suppression_list__v1.set_upstream(wait_for_acoustic__contact_raw__v1)
