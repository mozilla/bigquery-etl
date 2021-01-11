# Generated via https://github.com/mozilla/bigquery-etl/blob/master/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from airflow.operators.sensors import ExternalTaskSensor
import datetime
from utils.gcp import bigquery_etl_query, gke_command

default_args = {
    "owner": "dthorn@mozilla.com",
    "start_date": datetime.datetime(2020, 6, 29, 0, 0),
    "end_date": None,
    "email": ["telemetry-alerts@mozilla.com", "dthorn@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
}

with DAG(
    "bqetl_deletion_request_volume",
    default_args=default_args,
    schedule_interval="0 1 * * *",
) as dag:

    monitoring__deletion_request_volume__v1 = bigquery_etl_query(
        task_id="monitoring__deletion_request_volume__v1",
        destination_table="deletion_request_volume_v1",
        dataset_id="monitoring",
        project_id="moz-fx-data-shared-prod",
        owner="dthorn@mozilla.com",
        email=["dthorn@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        dag=dag,
    )
