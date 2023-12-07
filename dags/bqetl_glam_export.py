# Generated via https://github.com/mozilla/bigquery-etl/blob/main/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskMarker
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
import datetime
from utils.constants import ALLOWED_STATES, FAILED_STATES
from utils.gcp import bigquery_etl_query, gke_command, bigquery_dq_check

docs = """
### bqetl_glam_export

Built from bigquery-etl repo, [`dags/bqetl_glam_export.py`](https://github.com/mozilla/bigquery-etl/blob/main/dags/bqetl_glam_export.py)

#### Description

DAG to prepare GLAM data for public export.
#### Owner

ascholtz@mozilla.com
"""


default_args = {
    "owner": "ascholtz@mozilla.com",
    "start_date": datetime.datetime(2023, 11, 28, 0, 0),
    "end_date": None,
    "email": ["ascholtz@mozilla.com", "efilho@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
}

tags = ["impact/tier_2", "repo/bigquery-etl"]

with DAG(
    "bqetl_glam_export",
    default_args=default_args,
    schedule_interval="0 8 * * *",
    doc_md=docs,
    tags=tags,
) as dag:
    glam_derived__client_probe_counts_firefox_desktop_beta__v1 = bigquery_etl_query(
        task_id="glam_derived__client_probe_counts_firefox_desktop_beta__v1",
        destination_table="client_probe_counts_firefox_desktop_beta_v1",
        dataset_id="glam_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "efilho@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    glam_derived__client_probe_counts_firefox_desktop_nightly__v1 = bigquery_etl_query(
        task_id="glam_derived__client_probe_counts_firefox_desktop_nightly__v1",
        destination_table="client_probe_counts_firefox_desktop_nightly_v1",
        dataset_id="glam_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "efilho@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    wait_for_extracts = ExternalTaskSensor(
        task_id="wait_for_extracts",
        external_dag_id="glam",
        external_task_id="extracts",
        execution_delta=datetime.timedelta(seconds=21600),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    glam_derived__client_probe_counts_firefox_desktop_beta__v1.set_upstream(
        wait_for_extracts
    )

    glam_derived__client_probe_counts_firefox_desktop_nightly__v1.set_upstream(
        wait_for_extracts
    )
