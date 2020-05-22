# Generated via query_scheduling/generate_airflow_dags 

from airflow import DAG
from airflow.operators.sensors import ExternalTaskSensor
import datetime
from utils.gcp import bigquery_etl_query

default_args = {'owner': 'bewu@mozilla.com', 'email': ['telemetry-alerts@mozilla.com', 'bewu@mozilla.com', 'wlachance@mozilla.com'], 'depends_on_past': False, 'start_date': datetime.datetime(2019, 11, 1, 0, 0), 'retry_delay': 'datetime.timedelta(seconds=1200)', 'email_on_failure': True, 'email_on_retry': True, 'retries': 1}

with DAG('bqetl_error_aggregates', default_args=default_args, schedule_interval=datetime.timedelta(seconds=10800)) as dag:

    telemetry_derived__error_aggregates__v1 = bigquery_etl_query(
        destination_table='error_aggregates',
        dataset_id='telemetry_derived',
        project_id='moz-fx-data-shared-prod',
        owner='bewu@mozilla.com',
        email=['bewu@mozilla.com'],
        depends_on_past=False,
        dag=dag,
    )
    
    