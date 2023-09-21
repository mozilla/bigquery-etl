# Generated via https://github.com/mozilla/bigquery-etl/blob/main/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskMarker
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
import datetime
from utils.constants import ALLOWED_STATES, FAILED_STATES
from utils.gcp import bigquery_etl_query, gke_command, bigquery_dq_check

docs = """
### bqetl_reference

Built from bigquery-etl repo, [`dags/bqetl_reference.py`](https://github.com/mozilla/bigquery-etl/blob/main/dags/bqetl_reference.py)

#### Description

DAG to build reference data
#### Owner

cmorales@mozilla.com
"""


default_args = {
    "owner": "cmorales@mozilla.com",
    "start_date": datetime.datetime(2023, 9, 18, 0, 0),
    "end_date": None,
    "email": ["telemetry-alerts@mozilla.com", "cmorales@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
}

tags = ["impact/tier_1", "repo/bigquery-etl"]

with DAG(
    "bqetl_reference",
    default_args=default_args,
    schedule_interval="@daily",
    doc_md=docs,
    tags=tags,
) as dag:
    reference_derived__macroeconomic_indices__v1 = gke_command(
        task_id="reference_derived__macroeconomic_indices__v1",
        command=[
            "python",
            "sql/moz-fx-data-shared-prod/reference_derived/macroeconomic_indices_v1/query.py",
        ]
        + ["--market-date={{ ds }}", "--api-key={{ var.value.fmp_api_key }}"],
        docker_image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
        owner="cmorales@mozilla.com",
        email=[
            "cmorales@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "xluo@mozilla.com",
        ],
    )
