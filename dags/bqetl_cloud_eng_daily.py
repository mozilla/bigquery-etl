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
### bqetl_cloud_eng_daily

Built from bigquery-etl repo, [`dags/bqetl_cloud_eng_daily.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_cloud_eng_daily.py)

#### Description

Daily task runner for Cloud Eng bqetl tasks
#### Owner

wstuckey@mozilla.com

#### Tags

* impact/tier_3
* repo/bigquery-etl
* triage/no_triage
"""


default_args = {
    "owner": "wstuckey@mozilla.com",
    "start_date": datetime.datetime(2026, 3, 4, 0, 0),
    "end_date": None,
    "email": ["wstuckey@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
    "max_active_tis_per_dag": None,
}

tags = ["impact/tier_3", "repo/bigquery-etl", "triage/no_triage"]

with DAG(
    "bqetl_cloud_eng_daily",
    default_args=default_args,
    schedule_interval="@daily",
    doc_md=docs,
    tags=tags,
    catchup=False,
) as dag:

    mozcloud_derived__service_uptime__v1 = GKEPodOperator(
        task_id="mozcloud_derived__service_uptime__v1",
        arguments=[
            "python",
            "sql/moz-fx-data-shared-prod/mozcloud_derived/service_uptime_v1/query.py",
        ]
        + [],
        image="us-docker.pkg.dev/moz-fx-data-artifacts-prod/bigquery-etl/bigquery-etl:latest",
        owner="wstuckey@mozilla.com",
        email=["wstuckey@mozilla.com"],
    )
