# Generated via https://github.com/mozilla/bigquery-etl/blob/master/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from airflow.operators.sensors import ExternalTaskSensor
import datetime
from utils.gcp import bigquery_etl_query

default_args = {
    "owner": "jklukas@mozilla.com",
    "start_date": datetime.datetime(2020, 6, 1, 0, 0),
    "email": ["telemetry-alerts@mozilla.com", "jklukas@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
}

with DAG(
    "bqetl_amo_stats", default_args=default_args, schedule_interval="0 1 * * *"
) as dag:

    amo_dev__amo_stats_dau__v1 = bigquery_etl_query(
        task_id="amo_dev__amo_stats_dau__v1",
        destination_table="amo_stats_dau_v1",
        dataset_id="amo_dev",
        project_id="moz-fx-data-shared-prod",
        owner="jklukas@mozilla.com",
        email=["jklukas@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        dag=dag,
    )

    amo_dev__amo_stats_installs__v1 = bigquery_etl_query(
        task_id="amo_dev__amo_stats_installs__v1",
        destination_table="amo_stats_installs_v1",
        dataset_id="amo_dev",
        project_id="moz-fx-data-shared-prod",
        owner="jklukas@mozilla.com",
        email=["jklukas@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        dag=dag,
    )

    amo_prod__amo_stats_dau__v1 = bigquery_etl_query(
        task_id="amo_prod__amo_stats_dau__v1",
        destination_table="amo_stats_dau_v1",
        dataset_id="amo_prod",
        project_id="moz-fx-data-shared-prod",
        owner="jklukas@mozilla.com",
        email=["jklukas@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        dag=dag,
    )

    amo_prod__amo_stats_installs__v1 = bigquery_etl_query(
        task_id="amo_prod__amo_stats_installs__v1",
        destination_table="amo_stats_installs_v1",
        dataset_id="amo_prod",
        project_id="moz-fx-data-shared-prod",
        owner="jklukas@mozilla.com",
        email=["jklukas@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        dag=dag,
    )

    amo_dev__amo_stats_dau__v1.set_upstream(amo_prod__amo_stats_dau__v1)

    amo_dev__amo_stats_installs__v1.set_upstream(amo_prod__amo_stats_installs__v1)

    wait_for_main_summary_clients_daily = ExternalTaskSensor(
        task_id="wait_for_main_summary_clients_daily",
        external_dag_id="main_summary",
        external_task_id="clients_daily",
        check_existence=True,
        mode="reschedule",
        dag=dag,
    )

    amo_prod__amo_stats_dau__v1.set_upstream(wait_for_main_summary_clients_daily)

    amo_prod__amo_stats_installs__v1.set_upstream(wait_for_main_summary_clients_daily)
