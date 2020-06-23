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
    "bqetl_desktop", default_args=default_args, schedule_interval="0 1 * * *"
) as dag:

    firefox_desktop_exact_mau28_by_client_count_dimensions = bigquery_etl_query(
        task_id="firefox_desktop_exact_mau28_by_client_count_dimensions",
        destination_table="firefox_desktop_exact_mau28_by_client_count_dimensions_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jklukas@mozilla.com",
        email=["jklukas@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        dag=dag,
    )

    firefox_desktop_exact_mau28_by_dimensions = bigquery_etl_query(
        task_id="firefox_desktop_exact_mau28_by_dimensions",
        destination_table="firefox_desktop_exact_mau28_by_dimensions_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="relud@mozilla.com",
        email=["relud@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        dag=dag,
    )

    wait_for_telemetry_derived__clients_last_seen__v1 = ExternalTaskSensor(
        task_id="wait_for_telemetry_derived__clients_last_seen__v1",
        external_dag_id="bqetl_clients",
        external_task_id="telemetry_derived__clients_last_seen__v1",
        check_existence=True,
        mode="reschedule",
    )

    firefox_desktop_exact_mau28_by_client_count_dimensions.set_upstream(
        wait_for_telemetry_derived__clients_last_seen__v1
    )

    firefox_desktop_exact_mau28_by_dimensions.set_upstream(
        wait_for_telemetry_derived__clients_last_seen__v1
    )
