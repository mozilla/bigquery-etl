# Generated via https://github.com/mozilla/bigquery-etl/blob/master/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from airflow.operators.sensors import ExternalTaskSensor
import datetime
from utils.gcp import bigquery_etl_query

default_args = {
    "owner": "bewu@mozilla.com",
    "start_date": datetime.datetime(2018, 11, 27, 0, 0),
    "email": ["telemetry-alerts@mozilla.com", "bewu@mozilla.com", "frank@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
}

with DAG(
    "bqetl_search", default_args=default_args, schedule_interval="0 1 * * *"
) as dag:

    search_derived__search_aggregates__v8 = bigquery_etl_query(
        task_id="search_derived__search_aggregates__v8",
        destination_table="search_aggregates_v8",
        dataset_id="search_derived",
        project_id="moz-fx-data-shared-prod",
        owner="bewu@mozilla.com",
        email=["bewu@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        dag=dag,
    )

    search_derived__search_clients_last_seen__v1 = bigquery_etl_query(
        task_id="search_derived__search_clients_last_seen__v1",
        destination_table="search_clients_last_seen_v1",
        dataset_id="search_derived",
        project_id="moz-fx-data-shared-prod",
        owner="frank@mozilla.com",
        email=["frank@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=True,
        dag=dag,
    )

    search_derived__search_clients_daily__v8 = bigquery_etl_query(
        task_id="search_derived__search_clients_daily__v8",
        destination_table="search_clients_daily_v8",
        dataset_id="search_derived",
        project_id="moz-fx-data-shared-prod",
        owner="bewu@mozilla.com",
        email=["bewu@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        dag=dag,
    )

    search_derived__search_aggregates__v8.set_upstream(
        search_derived__search_clients_daily__v8
    )

    search_derived__search_clients_last_seen__v1.set_upstream(
        search_derived__search_clients_daily__v8
    )

    wait_for_main_summary_main_summary = ExternalTaskSensor(
        task_id="wait_for_main_summary_main_summary",
        external_dag_id="main_summary",
        external_task_id="main_summary",
        check_existence=True,
        mode="reschedule",
        dag=dag,
    )

    search_derived__search_clients_daily__v8.set_upstream(
        wait_for_main_summary_main_summary
    )
