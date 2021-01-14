# Generated via https://github.com/mozilla/bigquery-etl/blob/master/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from airflow.operators.sensors import ExternalTaskSensor
import datetime
from utils.gcp import bigquery_etl_query, gke_command

default_args = {
    "owner": "ascholtz@mozilla.com",
    "start_date": datetime.datetime(2021, 1, 10, 0, 0),
    "end_date": None,
    "email": ["telemetry-alerts@mozilla.com", "ascholtz@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=600),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
}

with DAG(
    "bqetl_experiments_hourly",
    default_args=default_args,
    schedule_interval="30 * * * *",
) as dag:

    experiment_enrollment_aggregates_hourly = bigquery_etl_query(
        task_id="experiment_enrollment_aggregates_hourly",
        destination_table='experiment_enrollment_aggregates_hourly_v1${{ macros.ds_format(execution_date + macros.timedelta(hours=-1), "%Y-%m-%d %H:%M:%S", "%Y%m%d%H") }}',
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        parameters=[
            'submission_timestamp:TIMESTAMP:{{ macros.ds_format(ts_nodash, "%Y%m%dT%H%M%S", "%Y-%m-%d %H:00:00") }}'
        ],
        sql_file_path="sql/moz-fx-data-shared-prod/telemetry_derived/experiment_enrollment_aggregates_hourly_v1/query.sql",
        dag=dag,
    )
