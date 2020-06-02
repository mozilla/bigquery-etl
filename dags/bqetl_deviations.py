# Generated via query_scheduling/generate_airflow_dags

from airflow import DAG
from airflow.operators.sensors import ExternalTaskSensor
import datetime
from utils.gcp import bigquery_etl_query

default_args = {
    "owner": "ascholtz@mozilla.com",
    "start_date": datetime.datetime(2020, 3, 29, 0, 0),
    "email": [
        "telemetry-alerts@mozilla.com",
        "ascholtz@mozilla.com",
        "jmccrosky@mozilla.com",
    ],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
}

with DAG(
    "bqetl_deviations", default_args=default_args, schedule_interval="0 1 * * *"
) as dag:

    telemetry_derived__deviations__v1 = bigquery_etl_query(
        task_id="telemetry_derived__deviations__v1",
        destination_table="deviations_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jmccrosky@mozilla.com",
        email=["jmccrosky@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        dag=dag,
    )

    wait_for_public_analysis_anomdtct = ExternalTaskSensor(
        task_id="wait_for_public_analysis_anomdtct",
        external_dag_id="public_analysis",
        external_task_id="anomdtct",
        dag=dag,
    )

    telemetry_derived__deviations__v1.set_upstream(wait_for_public_analysis_anomdtct)
