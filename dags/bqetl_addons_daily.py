# Generated via query_scheduling/generate_airflow_dags

from airflow import DAG
from airflow.operators.sensors import ExternalTaskSensor
import datetime
from utils.gcp import bigquery_etl_query

default_args = {
    "owner": "bmiroglio@mozilla.com",
    "start_date": datetime.datetime(2020, 3, 16, 0, 0),
    "email": ["telemetry-alerts@mozilla.com", "bmiroglio@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
}

with DAG(
    "bqetl_addons_daily", default_args=default_args, schedule_interval="0 1 * * *"
) as dag:

    telemetry_derived__addons_daily__v1 = bigquery_etl_query(
        task_id="telemetry_derived__addons_daily__v1",
        destination_table="addons_daily_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="bmiroglio@mozilla.com",
        email=["bmiroglio@mozilla.com"],
        depends_on_past=False,
        dag=dag,
    )

    wait_for_search_clients_daily_bigquery = ExternalTaskSensor(
        task_id="wait_for_search_clients_daily_bigquery",
        external_dag_id="main_summary",
        external_task_id="search_clients_daily_bigquery",
        dag=dag,
    )

    telemetry_derived__addons_daily__v1.set_upstream(
        wait_for_search_clients_daily_bigquery
    )

    wait_for_clients_last_seen = ExternalTaskSensor(
        task_id="wait_for_clients_last_seen",
        external_dag_id="main_summary",
        external_task_id="clients_last_seen",
        dag=dag,
    )

    telemetry_derived__addons_daily__v1.set_upstream(wait_for_clients_last_seen)
