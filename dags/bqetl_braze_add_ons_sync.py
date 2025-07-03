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
### bqetl_braze_add_ons_sync

Built from bigquery-etl repo, [`dags/bqetl_braze_add_ons_sync.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_braze_add_ons_sync.py)

#### Description

Create Add-ons sync table to sync to Braze.

#### Owner

cbeck@mozilla.com

#### Tags

* impact/tier_3
* repo/bigquery-etl
"""


default_args = {
    "owner": "cbeck@mozilla.com",
    "start_date": datetime.datetime(2025, 7, 3, 0, 0),
    "end_date": None,
    "email": ["cbeck@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=300),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
    "max_active_tis_per_dag": None,
}

tags = ["impact/tier_3", "repo/bigquery-etl"]

with DAG(
    "bqetl_braze_add_ons_sync",
    default_args=default_args,
    schedule_interval="@once",
    doc_md=docs,
    tags=tags,
    catchup=False,
) as dag:

    wait_for_braze_derived__add_ons_developers__v1 = ExternalTaskSensor(
        task_id="wait_for_braze_derived__add_ons_developers__v1",
        external_dag_id="bqetl_braze_add_ons_devs_users",
        external_task_id="braze_derived__add_ons_developers__v1",
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    braze_external__add_ons_devs_users_sync__v1 = bigquery_etl_query(
        task_id="braze_external__add_ons_devs_users_sync__v1",
        destination_table="add_ons_devs_users_sync_v1",
        dataset_id="braze_external",
        project_id="moz-fx-data-shared-prod",
        owner="cbeck@mozilla.com",
        email=["cbeck@mozilla.com", "lmcfall@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    braze_external__add_ons_devs_users_sync__v1.set_upstream(
        wait_for_braze_derived__add_ons_developers__v1
    )
