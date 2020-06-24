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
    "bqetl_clients_daily", default_args=default_args, schedule_interval="0 1 * * *"
) as dag:

    telemetry_derived__clients_first_seen__v1 = bigquery_etl_query(
        task_id="telemetry_derived__clients_first_seen__v1",
        destination_table="clients_first_seen_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jklukas@mozilla.com",
        email=["jklukas@mozilla.com"],
        start_date=datetime.datetime(2020, 5, 5, 0, 0),
        date_partition_parameter=None,
        depends_on_past=True,
        parameters=["submission_date:DATE:{{ds}}"],
        dag=dag,
    )

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

    telemetry_derived__clients_daily__v6 = bigquery_etl_query(
        task_id="telemetry_derived__clients_daily__v6",
        destination_table="clients_daily_v6",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="dthorn@mozilla.com",
        email=["dthorn@mozilla.com"],
        start_date=datetime.datetime(2019, 11, 5, 0, 0),
        date_partition_parameter="submission_date",
        depends_on_past=False,
        dag=dag,
    )

    telemetry_derived__clients_last_seen__v1 = bigquery_etl_query(
        task_id="telemetry_derived__clients_last_seen__v1",
        destination_table="clients_last_seen_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="dthorn@mozilla.com",
        email=["dthorn@mozilla.com", "jklukas@mozilla.com"],
        start_date=datetime.datetime(2019, 4, 15, 0, 0),
        date_partition_parameter="submission_date",
        depends_on_past=True,
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

    telemetry_derived__clients_first_seen__v1.set_upstream(
        telemetry_derived__clients_daily__v6
    )

    firefox_desktop_exact_mau28_by_client_count_dimensions.set_upstream(
        telemetry_derived__clients_last_seen__v1
    )

    wait_for_telemetry_derived__main_summary__v4 = ExternalTaskSensor(
        task_id="wait_for_telemetry_derived__main_summary__v4",
        external_dag_id="bqetl_main_summary",
        external_task_id="telemetry_derived__main_summary__v4",
        check_existence=True,
        mode="reschedule",
    )

    telemetry_derived__clients_daily__v6.set_upstream(
        wait_for_telemetry_derived__main_summary__v4
    )
    wait_for_main_summary_main_summary = ExternalTaskSensor(
        task_id="wait_for_main_summary_main_summary",
        external_dag_id="main_summary",
        external_task_id="main_summary",
        check_existence=True,
        mode="reschedule",
        dag=dag,
    )

    telemetry_derived__clients_daily__v6.set_upstream(
        wait_for_main_summary_main_summary
    )

    telemetry_derived__clients_last_seen__v1.set_upstream(
        telemetry_derived__clients_daily__v6
    )

    firefox_desktop_exact_mau28_by_dimensions.set_upstream(
        telemetry_derived__clients_last_seen__v1
    )
