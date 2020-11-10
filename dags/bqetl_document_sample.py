# Generated via https://github.com/mozilla/bigquery-etl/blob/master/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from airflow.operators.sensors import ExternalTaskSensor
import datetime
from utils.gcp import bigquery_etl_query, gke_command

default_args = {
    "owner": "amiyaguchi@mozilla.com",
    "start_date": datetime.datetime(2020, 2, 17, 0, 0),
    "email": ["telemetry-alerts@mozilla.com", "amiyaguchi@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
}

with DAG(
    "bqetl_document_sample", default_args=default_args, schedule_interval="@daily"
) as dag:

    monitoring__document_sample_nonprod__v1 = bigquery_etl_query(
        task_id="monitoring__document_sample_nonprod__v1",
        destination_table="document_sample_nonprod_v1",
        dataset_id="monitoring",
        project_id="moz-fx-data-shared-prod",
        owner="amiyaguchi@mozilla.com",
        email=["amiyaguchi@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        arguments=["--append_table"],
        dag=dag,
    )
