# Generated via https://github.com/mozilla/bigquery-etl/blob/main/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskMarker
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
import datetime
from utils.constants import ALLOWED_STATES, FAILED_STATES
from utils.gcp import bigquery_etl_query, gke_command

docs = """
### bqetl_newtab

Built from bigquery-etl repo, [`dags/bqetl_newtab.py`](https://github.com/mozilla/bigquery-etl/blob/main/dags/bqetl_newtab.py)

#### Description

Schedules newtab related queries.
#### Owner

anicholson@mozilla.com
"""


default_args = {
    "owner": "anicholson@mozilla.com",
    "start_date": datetime.datetime(2022, 7, 1, 0, 0),
    "end_date": None,
    "email": ["anicholson@mozilla.com", "anicholson@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
}

tags = ["impact/tier_1", "repo/bigquery-etl"]

with DAG(
    "bqetl_newtab",
    default_args=default_args,
    schedule_interval="@daily",
    doc_md=docs,
    tags=tags,
) as dag:

    telemetry_derived__newtab_interactions__v1 = bigquery_etl_query(
        task_id="telemetry_derived__newtab_interactions__v1",
        destination_table="newtab_interactions_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="anicholson@mozilla.com",
        email=["anicholson@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )
