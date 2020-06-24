# Generated via https://github.com/mozilla/bigquery-etl/blob/master/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from airflow.operators.sensors import ExternalTaskSensor
import datetime
from utils.gcp import bigquery_etl_query

default_args = {
    "owner": "ssuh@mozilla.com",
    "start_date": datetime.datetime(2018, 11, 27, 0, 0),
    "email": ["telemetry-alerts@mozilla.com", "ssuh@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
}

with DAG(
    "bqetl_experiments", default_args=default_args, schedule_interval="0 1 * * *"
) as dag:

    telemetry_derived__experiments_daily_active_clients__v1 = bigquery_etl_query(
        task_id="telemetry_derived__experiments_daily_active_clients__v1",
        destination_table="experiments_daily_active_clients_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ssuh@mozilla.com",
        email=["ssuh@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        dag=dag,
    )

    wait_for_telemetry_derived__clients_daily__v6 = ExternalTaskSensor(
        task_id="wait_for_telemetry_derived__clients_daily__v6",
        external_dag_id="bqetl_clients_daily",
        external_task_id="telemetry_derived__clients_daily__v6",
        check_existence=True,
        mode="reschedule",
    )

    telemetry_derived__experiments_daily_active_clients__v1.set_upstream(
        wait_for_telemetry_derived__clients_daily__v6
    )
