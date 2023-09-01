# Generated via https://github.com/mozilla/bigquery-etl/blob/main/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskMarker
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
import datetime
from utils.constants import ALLOWED_STATES, FAILED_STATES
from utils.gcp import bigquery_etl_query, gke_command, bigquery_dq_check

docs = """
### bqetl_jobs_by_org_derived

Built from bigquery-etl repo, [`dags/bqetl_jobs_by_org_derived.py`](https://github.com/mozilla/bigquery-etl/blob/main/dags/bqetl_jobs_by_org_derived.py)

#### Description

This DAG schedules queries to populate jobs by organization tables from mozdata, moz-fx-data-shared-prod, moz-fx-data-marketing-prod, moz-fx-data-bq-data-science
#### Owner

mhirose@mozilla.com
"""


default_args = {
    "owner": "mhirose@mozilla.com",
    "start_date": datetime.datetime(2023, 9, 1, 0, 0),
    "end_date": None,
    "email": ["telemetry-alerts@mozilla.com", "mhirose@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
}

tags = ["impact/tier_2", "repo/bigquery-etl"]

with DAG(
    "bqetl_jobs_by_org_derived",
    default_args=default_args,
    schedule_interval="0 4 * * *",
    doc_md=docs,
    tags=tags,
) as dag:
    monitoring_derived__jobs_by_organization__v1 = gke_command(
        task_id="monitoring_derived__jobs_by_organization__v1",
        command=[
            "python",
            "sql/moz-fx-data-shared-prod/monitoring_derived/jobs_by_organization_v1/query.py",
        ]
        + [],
        docker_image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
        owner="mhirose@mozilla.com",
        email=["mhirose@mozilla.com", "telemetry-alerts@mozilla.com"],
    )
