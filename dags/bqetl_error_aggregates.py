# Generated via query_scheduling/generate_airflow_dags 

from airflow import DAG
from airflow.operators.sensors import ExternalTaskSensor
from datetime import datetime, timedelta
from utils.gcp import bigquery_etl_query


with DAG("bqetl_error_aggregates", default_args={'owner': 'bewu@mozilla.com', 'email': ['telemetry-alerts@mozilla.com', 'bewu@mozilla.com', 'wlachance@mozilla.com'], 'start_date': '2019-11-01', 'retries': 1}, schedule_interval="hourly") as dag:

    telemetry_derived__error_aggregates__v1 = bigquery_etl_query(
        destination_table="error_aggregates",
        dataset_id="telemetry_derived",
        project_id='moz-fx-data-shared-prod',
        dag=dag,
    )

    
