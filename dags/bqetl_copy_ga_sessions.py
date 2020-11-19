# Generated via https://github.com/mozilla/bigquery-etl/blob/master/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from airflow.operators.sensors import ExternalTaskSensor
import datetime
from utils.gcp import bigquery_etl_query, gke_command

default_args = {
    "owner": "bewu@mozilla.com",
    "start_date": datetime.datetime(2020, 10, 31, 0, 0),
    "email": ["bewu@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
}

with DAG(
    "bqetl_copy_ga_sessions", default_args=default_args, schedule_interval="0 1 * * *"
) as dag:

    ga_sessions__copy_ga_sessions__v1 = gke_command(
        task_id="ga_sessions__copy_ga_sessions__v1",
        command=[
            "python",
            "sql/moz-fx-data-marketing-prod/ga_sessions/copy_ga_sessions_v1/query.py",
        ]
        + [
            "--start-date",
            "{{ ds }}",
            "--src-project",
            "ga-mozilla-org-prod-001",
            "--dst-project",
            "moz-fx-data-marketing-prod",
            "--overwrite",
            "65789850",
            "66602784",
            "65912487",
            "180612539",
            "220432379",
            "65887927",
            "66726481",
        ],
        docker_image="mozilla/bigquery-etl:latest",
        owner="bewu@mozilla.com",
        email=["bewu@mozilla.com"],
    )
