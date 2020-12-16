# Generated via https://github.com/mozilla/bigquery-etl/blob/master/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from airflow.operators.sensors import ExternalTaskSensor
import datetime
from utils.gcp import bigquery_etl_query, gke_command

default_args = {
    "owner": "ascholtz@mozilla.com",
    "start_date": datetime.datetime(2020, 10, 1, 0, 0),
    "end_date": datetime.datetime(2021, 1, 1, 0, 0),
    "email": [
        "telemetry-alerts@mozilla.com",
        "ascholtz@mozilla.com",
        "aplacitelli@mozilla.com",
    ],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
}

with DAG(
    "bqetl_deviations", default_args=default_args, schedule_interval="0 4 * * *"
) as dag:

    telemetry_derived__deviations__v1 = bigquery_etl_query(
        task_id="telemetry_derived__deviations__v1",
        destination_table="deviations_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=[
            "aplacitelli@mozilla.com",
            "ascholtz@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        dag=dag,
    )

    wait_for_anomdtct_anomdtct = ExternalTaskSensor(
        task_id="wait_for_anomdtct_anomdtct",
        external_dag_id="anomdtct",
        external_task_id="anomdtct",
        execution_delta=datetime.timedelta(seconds=3600),
        check_existence=True,
        mode="reschedule",
        pool="DATA_ENG_EXTERNALTASKSENSOR",
        dag=dag,
    )

    telemetry_derived__deviations__v1.set_upstream(wait_for_anomdtct_anomdtct)
