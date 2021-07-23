# Generated via https://github.com/mozilla/bigquery-etl/blob/main/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from operators.task_sensor import ExternalTaskCompletedSensor
import datetime
from utils.gcp import bigquery_etl_query, gke_command

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

with DAG(
    "bqetl_experimenter_experiments_import",
    default_args=default_args,
    schedule_interval="*/10 * * * *",
    doc_md=docs,
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
