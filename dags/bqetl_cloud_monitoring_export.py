# Generated via https://github.com/mozilla/bigquery-etl/blob/master/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from airflow.operators.sensors import ExternalTaskSensor
import datetime
from utils.gcp import bigquery_etl_query, gke_command

default_args = {
    "owner": "bewu@mozilla.com",
    "start_date": datetime.datetime(2021, 1, 1, 0, 0),
    "end_date": datetime.datetime(2021, 1, 1, 0, 0),
    "email": ["bewu@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=900),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
}

with DAG(
    "bqetl_cloud_monitoring_export",
    default_args=default_args,
    schedule_interval="0 * * * *",
) as dag:

    monitoring__dataflow_user_counters__v1 = gke_command(
        task_id="monitoring__dataflow_user_counters__v1",
        command=[
            "python",
            "sql/moz-fx-data-shared-prod/monitoring/dataflow_user_counters_v1/query.py",
        ]
        + [
            "--execution-time",
            "{{ ts }}",
            "--interval-hours",
            "1",
            "--time-offset",
            "1",
        ],
        docker_image="mozilla/bigquery-etl:latest",
        owner="bewu@mozilla.com",
        email=["bewu@mozilla.com"],
    )

    monitoring__kubernetes_not_coerced_to_int__v1 = gke_command(
        task_id="monitoring__kubernetes_not_coerced_to_int__v1",
        command=[
            "python",
            "sql/moz-fx-data-shared-prod/monitoring/kubernetes_not_coerced_to_int_v1/query.py",
        ]
        + [
            "--execution-time",
            "{{ ts }}",
            "--interval-hours",
            "1",
            "--time-offset",
            "1",
        ],
        docker_image="mozilla/bigquery-etl:latest",
        owner="bewu@mozilla.com",
        email=["bewu@mozilla.com"],
    )

    monitoring__pubsub_subscription_oldest_unacked_msg__v1 = gke_command(
        task_id="monitoring__pubsub_subscription_oldest_unacked_msg__v1",
        command=[
            "python",
            "sql/moz-fx-data-shared-prod/monitoring/pubsub_subscription_oldest_unacked_msg_v1/query.py",
        ]
        + [
            "--execution-time",
            "{{ ts }}",
            "--interval-hours",
            "1",
            "--time-offset",
            "1",
        ],
        docker_image="mozilla/bigquery-etl:latest",
        owner="bewu@mozilla.com",
        email=["bewu@mozilla.com"],
    )
