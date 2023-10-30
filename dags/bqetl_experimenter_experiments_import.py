# Generated via https://github.com/mozilla/bigquery-etl/blob/main/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskMarker
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
import datetime
from utils.constants import ALLOWED_STATES, FAILED_STATES
from utils.gcp import bigquery_etl_query, gke_command, bigquery_dq_check

docs = """
### bqetl_experimenter_experiments_import

Built from bigquery-etl repo, [`dags/bqetl_experimenter_experiments_import.py`](https://github.com/mozilla/bigquery-etl/blob/main/dags/bqetl_experimenter_experiments_import.py)

#### Description

Imports experiments from the Experimenter V4 and V6 API.

Imported experiment data is used for experiment monitoring in
[Grafana](https://grafana.telemetry.mozilla.org/d/XspgvdxZz/experiment-enrollment).

#### Owner

ascholtz@mozilla.com
"""


default_args = {
    "owner": "ascholtz@mozilla.com",
    "start_date": datetime.datetime(2020, 10, 9, 0, 0),
    "end_date": None,
    "email": ["ascholtz@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 0,
}

tags = ["impact/tier_2", "repo/bigquery-etl"]

with DAG(
    "bqetl_experimenter_experiments_import",
    default_args=default_args,
    schedule_interval="*/10 * * * *",
    doc_md=docs,
    tags=tags,
) as dag:
    monitoring__experimenter_experiments__v1 = gke_command(
        task_id="monitoring__experimenter_experiments__v1",
        command=[
            "python",
            "sql/moz-fx-data-experiments/monitoring/experimenter_experiments_v1/query.py",
        ]
        + [],
        docker_image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com"],
    )

    with TaskGroup(
        "monitoring__experimenter_experiments__v1_external"
    ) as monitoring__experimenter_experiments__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_monitoring__wait_for_monitoring__experimenter_experiments__v1",
            external_dag_id="bqetl_monitoring",
            external_task_id="wait_for_monitoring__experimenter_experiments__v1",
        )

        monitoring__experimenter_experiments__v1_external.set_upstream(
            monitoring__experimenter_experiments__v1
        )
