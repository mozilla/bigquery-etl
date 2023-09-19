# Generated via https://github.com/mozilla/bigquery-etl/blob/main/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskMarker
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
import datetime
from utils.constants import ALLOWED_STATES, FAILED_STATES
from utils.gcp import bigquery_etl_query, gke_command, bigquery_dq_check

docs = """
### bqetl_domain_meta

Built from bigquery-etl repo, [`dags/bqetl_domain_meta.py`](https://github.com/mozilla/bigquery-etl/blob/main/dags/bqetl_domain_meta.py)

#### Description

Domain metadata
#### Owner

wstuckey@mozilla.com
"""


default_args = {
    "owner": "wstuckey@mozilla.com",
    "start_date": datetime.datetime(2022, 10, 13, 0, 0),
    "end_date": None,
    "email": ["wstuckey@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
}

tags = ["impact/tier_3", "repo/bigquery-etl", "triage/no_triage"]

with DAG(
    "bqetl_domain_meta",
    default_args=default_args,
    schedule_interval="@monthly",
    doc_md=docs,
    tags=tags,
) as dag:
    domain_metadata_derived__top_domains__v1 = bigquery_etl_query(
        task_id="domain_metadata_derived__top_domains__v1",
        destination_table="top_domains_v1",
        dataset_id="domain_metadata_derived",
        project_id="moz-fx-data-shared-prod",
        owner="wstuckey@mozilla.com",
        email=["wstuckey@mozilla.com"],
        date_partition_parameter="submission_date",
        table_partition_template='${{ dag_run.logical_date.strftime("%Y%m") }}',
        depends_on_past=False,
    )
