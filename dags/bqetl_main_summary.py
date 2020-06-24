# Generated via https://github.com/mozilla/bigquery-etl/blob/master/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from airflow.operators.sensors import ExternalTaskSensor
import datetime
from utils.gcp import bigquery_etl_query

default_args = {
    "owner": "dthorn@mozilla.com",
    "start_date": datetime.datetime(2018, 11, 27, 0, 0),
    "email": [
        "telemetry-alerts@mozilla.com",
        "dthorn@mozilla.com",
        "jklukas@mozilla.com",
        "frank@mozilla.com",
    ],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
}

with DAG(
    "bqetl_main_summary", default_args=default_args, schedule_interval="0 1 * * *"
) as dag:

    telemetry_derived__main_summary__v4 = bigquery_etl_query(
        task_id="telemetry_derived__main_summary__v4",
        destination_table="main_summary_v4",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="dthorn@mozilla.com",
        email=["dthorn@mozilla.com"],
        start_date=datetime.datetime(2019, 10, 25, 0, 0),
        date_partition_parameter="submission_date",
        depends_on_past=False,
        mulitpart=True,
        sql_file_path="sql/telemetry_derived/main_summary_v4",
        dag=dag,
    )

    wait_for_copy_deduplicate_copy_deduplicate_main_ping = ExternalTaskSensor(
        task_id="wait_for_copy_deduplicate_copy_deduplicate_main_ping",
        external_dag_id="copy_deduplicate",
        external_task_id="copy_deduplicate_main_ping",
        check_existence=True,
        mode="reschedule",
        dag=dag,
    )

    telemetry_derived__main_summary__v4.set_upstream(
        wait_for_copy_deduplicate_copy_deduplicate_main_ping
    )
