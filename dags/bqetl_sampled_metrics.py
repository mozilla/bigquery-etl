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
### bqetl_sampled_metrics

Built from bigquery-etl repo, [`dags/bqetl_sampled_metrics.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_sampled_metrics.py)

#### Description

Imports sampled Glean metrics from the Experimenter API.
Tracks which metrics are client-side sampled and their sample rates.

#### Owner

efilho@mozilla.com

#### Tags

* impact/tier_2
* repo/bigquery-etl
"""


default_args = {
    "owner": "efilho@mozilla.com",
    "start_date": datetime.datetime(2026, 2, 17, 0, 0),
    "end_date": None,
    "email": ["efilho@mozilla.com", "telemetry-alerts@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
    "max_active_tis_per_dag": None,
}

tags = ["impact/tier_2", "repo/bigquery-etl"]

with DAG(
    "bqetl_sampled_metrics",
    default_args=default_args,
    schedule_interval="0 4 * * *",
    doc_md=docs,
    tags=tags,
    catchup=False,
) as dag:

    telemetry_derived__sampled_metrics__v1 = GKEPodOperator(
        task_id="telemetry_derived__sampled_metrics__v1",
        arguments=[
            "python",
            "sql/moz-fx-data-shared-prod/telemetry_derived/sampled_metrics_v1/query.py",
        ]
        + [],
        image="us-docker.pkg.dev/moz-fx-data-artifacts-prod/bigquery-etl/bigquery-etl:latest",
        owner="efilho@mozilla.com",
        email=["efilho@mozilla.com", "telemetry-alerts@mozilla.com"],
    )
