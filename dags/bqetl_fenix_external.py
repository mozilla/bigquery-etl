# Generated via https://github.com/mozilla/bigquery-etl/blob/main/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskMarker
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
import datetime
from utils.constants import ALLOWED_STATES, FAILED_STATES
from utils.gcp import bigquery_etl_query, gke_command, bigquery_dq_check

docs = """
### bqetl_fenix_external

Built from bigquery-etl repo, [`dags/bqetl_fenix_external.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_fenix_external.py)

#### Owner

frank@mozilla.com

#### Tags

* impact/tier_1
* repo/bigquery-etl
"""


default_args = {
    "owner": "frank@mozilla.com",
    "start_date": datetime.datetime(2023, 5, 7, 0, 0),
    "end_date": None,
    "email": ["frank@mozilla.com", "telemetry-alerts@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
}

tags = ["impact/tier_1", "repo/bigquery-etl"]

with DAG(
    "bqetl_fenix_external",
    default_args=default_args,
    schedule_interval="0 2 * * *",
    doc_md=docs,
    tags=tags,
) as dag:
    fenix_external__gplay_installs_by_country__v1 = bigquery_etl_query(
        task_id="fenix_external__gplay_installs_by_country__v1",
        destination_table="gplay_installs_by_country_v1",
        dataset_id="fenix_external",
        project_id="moz-fx-data-shared-prod",
        owner="frank@mozilla.com",
        email=["frank@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    wait_for_play_store_export = ExternalTaskSensor(
        task_id="wait_for_play_store_export",
        external_dag_id="play_store_export",
        external_task_id="play_store_export",
        execution_delta=datetime.timedelta(seconds=7200),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    fenix_external__gplay_installs_by_country__v1.set_upstream(
        wait_for_play_store_export
    )
