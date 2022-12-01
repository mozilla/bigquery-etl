# Generated via https://github.com/mozilla/bigquery-etl/blob/main/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskMarker
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
import datetime
from utils.constants import ALLOWED_STATES, FAILED_STATES
from utils.gcp import bigquery_etl_query, gke_command

docs = """
### bqetl_org_mozilla_firefox_derived

Built from bigquery-etl repo, [`dags/bqetl_org_mozilla_firefox_derived.py`](https://github.com/mozilla/bigquery-etl/blob/main/dags/bqetl_org_mozilla_firefox_derived.py)

#### Owner

frank@mozilla.com
"""


default_args = {
    "owner": "frank@mozilla.com",
    "start_date": datetime.datetime(2022, 11, 30, 0, 0),
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
    "bqetl_org_mozilla_firefox_derived",
    default_args=default_args,
    schedule_interval="0 2 * * *",
    doc_md=docs,
    tags=tags,
) as dag:

    fenix_derived__clients_yearly__v1 = bigquery_etl_query(
        task_id="fenix_derived__clients_yearly__v1",
        destination_table="clients_yearly_v1",
        dataset_id="fenix_derived",
        project_id="moz-fx-data-shared-prod",
        owner="frank@mozilla.com",
        email=["frank@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=True,
    )
