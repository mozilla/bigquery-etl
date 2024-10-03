# Generated via https://github.com/mozilla/bigquery-etl/blob/main/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskMarker
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
import datetime
from operators.gcp_container_operator import GKEPodOperator
from utils.constants import ALLOWED_STATES, FAILED_STATES
from utils.gcp import bigquery_etl_query, bigquery_dq_check
from bigeye_airflow.operators.run_metrics_operator import RunMetricsOperator

docs = """
### bqetl_shredder_monitoring

Built from bigquery-etl repo, [`dags/bqetl_shredder_monitoring.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_shredder_monitoring.py)

#### Description

[EXPERIMENTAL] Monitoring queries for shredder operation
#### Owner

bewu@mozilla.com

#### Tags

* impact/tier_3
* repo/bigquery-etl
* triage/no_triage
"""


default_args = {
    "owner": "bewu@mozilla.com",
    "start_date": datetime.datetime(2024, 10, 1, 0, 0),
    "end_date": None,
    "email": ["bewu@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
}

tags = ["impact/tier_3", "repo/bigquery-etl", "triage/no_triage"]

with DAG(
    "bqetl_shredder_monitoring",
    default_args=default_args,
    schedule_interval="0 12 * * *",
    doc_md=docs,
    tags=tags,
) as dag:

    monitoring_derived__shredder_targets__v1 = GKEPodOperator(
        task_id="monitoring_derived__shredder_targets__v1",
        arguments=[
            "python",
            "sql/moz-fx-data-shared-prod/monitoring_derived/shredder_targets_v1/query.py",
        ]
        + [
            "--output-table",
            "moz-fx-data-shared-prod.monitoring_derived.shredder_targets_v1",
            "--run-date",
            "{{ ds }}",
        ],
        image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
        owner="bewu@mozilla.com",
        email=["bewu@mozilla.com"],
    )
