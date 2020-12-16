# Generated via https://github.com/mozilla/bigquery-etl/blob/master/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from airflow.operators.sensors import ExternalTaskSensor
import datetime
from utils.gcp import bigquery_etl_query, gke_command

default_args = {
    "owner": "chutten@mozilla.com",
    "start_date": datetime.datetime(2019, 7, 20, 0, 0),
    "end_date": None,
    "email": ["telemetry-alerts@mozilla.com", "chutten@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
}

with DAG(
    "bqetl_ssl_ratios", default_args=default_args, schedule_interval="0 2 * * *"
) as dag:

    telemetry_derived__ssl_ratios__v1 = bigquery_etl_query(
        task_id="telemetry_derived__ssl_ratios__v1",
        destination_table="ssl_ratios_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="chutten@mozilla.com",
        email=["chutten@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        dag=dag,
    )

    wait_for_copy_deduplicate_main_ping = ExternalTaskSensor(
        task_id="wait_for_copy_deduplicate_main_ping",
        external_dag_id="copy_deduplicate",
        external_task_id="copy_deduplicate_main_ping",
        execution_delta=datetime.timedelta(seconds=3600),
        check_existence=True,
        mode="reschedule",
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    telemetry_derived__ssl_ratios__v1.set_upstream(wait_for_copy_deduplicate_main_ping)
