# Generated via https://github.com/mozilla/bigquery-etl/blob/master/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from airflow.operators.sensors import ExternalTaskSensor
import datetime
from utils.gcp import bigquery_etl_query

default_args = {
    "owner": "jklukas@mozilla.com",
    "start_date": datetime.datetime(2019, 7, 25, 0, 0),
    "email": ["telemetry-alerts@mozilla.com", "jklukas@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=300),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
}

with DAG(
    "bqetl_activity_stream", default_args=default_args, schedule_interval="0 2 * * *"
) as dag:

    activity_stream_bi__impression_stats_flat__v1 = bigquery_etl_query(
        task_id="activity_stream_bi__impression_stats_flat__v1",
        destination_table="impression_stats_flat_v1",
        dataset_id="activity_stream_bi",
        project_id="moz-fx-data-shared-prod",
        owner="jklukas@mozilla.com",
        email=["jklukas@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        dag=dag,
    )

    activity_stream_bi__impression_stats_by_experiment__v1 = bigquery_etl_query(
        task_id="activity_stream_bi__impression_stats_by_experiment__v1",
        destination_table="impression_stats_by_experiment_v1",
        dataset_id="activity_stream_bi",
        project_id="moz-fx-data-shared-prod",
        owner="jklukas@mozilla.com",
        email=["jklukas@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        dag=dag,
    )

    wait_for_copy_deduplicate_all = ExternalTaskSensor(
        task_id="wait_for_copy_deduplicate_all",
        external_dag_id="copy_deduplicate",
        external_task_id="copy_deduplicate_all",
        execution_delta=datetime.timedelta(seconds=3600),
        check_existence=True,
        mode="reschedule",
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    activity_stream_bi__impression_stats_flat__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    activity_stream_bi__impression_stats_by_experiment__v1.set_upstream(
        activity_stream_bi__impression_stats_flat__v1
    )
