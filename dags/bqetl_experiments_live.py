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
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 0,
}

with DAG(
    "bqetl_experiments_live", default_args=default_args, schedule_interval="*/5 * * * *"
) as dag:

    experiment_enrollment_aggregates_recents = bigquery_etl_query(
        task_id="experiment_enrollment_aggregates_recents",
        destination_table="experiment_enrollment_aggregates_recents_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        parameters=["submission_timestamp:TIMESTAMP:{{ts}}"],
        dag=dag,
    )

    experiment_enrollment_cumulative_population_estimate = bigquery_etl_query(
        task_id="experiment_enrollment_cumulative_population_estimate",
        destination_table="experiment_enrollment_cumulative_population_estimate_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        dag=dag,
    )

    experiment_enrollment_cumulative_population_estimate.set_upstream(
        experiment_enrollment_aggregates_recents
    )
