# Generated via https://github.com/mozilla/bigquery-etl/blob/main/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from operators.task_sensor import ExternalTaskCompletedSensor
import datetime
from utils.gcp import bigquery_etl_query, gke_command

docs = """
### bqetl_iprospect

Built from bigquery-etl repo, [`dags/bqetl_iprospect.py`](https://github.com/mozilla/bigquery-etl/blob/main/dags/bqetl_iprospect.py)

#### Description

This DAG imports iProspect data from moz-fx-data-marketing-prod-iprospect.

#### Owner

ascholtz@mozilla.com
"""


default_args = {
    "owner": "ascholtz@mozilla.com",
    "start_date": datetime.datetime(2021, 4, 19, 0, 0),
    "end_date": None,
    "email": ["ascholtz@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
}

with DAG(
    "bqetl_iprospect",
    default_args=default_args,
    schedule_interval="0 4 * * *",
    doc_md=docs,
) as dag:

    iprospect__detail_export__v1 = gke_command(
        task_id="iprospect__detail_export__v1",
        command=[
            "python",
            "sql/moz-fx-data-marketing-prod/iprospect/detail_export_v1/query.py",
        ]
        + ["--date", "{{ ds }}"],
        docker_image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com"],
    )
