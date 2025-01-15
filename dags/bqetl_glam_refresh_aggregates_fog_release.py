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
### bqetl_glam_refresh_aggregates_fog_release

Built from bigquery-etl repo, [`dags/bqetl_glam_refresh_aggregates_fog_release.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_glam_refresh_aggregates_fog_release.py)

#### Description

Update GLAM FOG release tables that serve aggregated data.
#### Owner

efilho@mozilla.com

#### Tags

* impact/tier_2
* repo/bigquery-etl
"""


default_args = {
    "owner": "efilho@mozilla.com",
    "start_date": datetime.datetime(2024, 12, 10, 0, 0),
    "end_date": None,
    "email": ["efilho@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
}

tags = ["impact/tier_2", "repo/bigquery-etl"]

with DAG(
    "bqetl_glam_refresh_aggregates_fog_release",
    default_args=default_args,
    schedule_interval="0 18 * * 6",
    doc_md=docs,
    tags=tags,
) as dag:

    wait_for_fog_release_done = ExternalTaskSensor(
        task_id="wait_for_fog_release_done",
        external_dag_id="glam_fog_release",
        external_task_id="fog_release_done",
        execution_delta=datetime.timedelta(seconds=28800),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    glam_etl__glam_fog_release_aggregates__v1 = bigquery_etl_query(
        task_id="glam_etl__glam_fog_release_aggregates__v1",
        destination_table=None,
        dataset_id="glam_etl",
        project_id="moz-fx-glam-prod",
        owner="efilho@mozilla.com",
        email=["efilho@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        sql_file_path="sql/moz-fx-glam-prod/glam_etl/glam_fog_release_aggregates_v1/script.sql",
    )

    glam_etl__glam_fog_release_aggregates__v1.set_upstream(wait_for_fog_release_done)
