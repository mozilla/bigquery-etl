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
    "bqetl_nondesktop", default_args=default_args, schedule_interval="0 3 * * *"
) as dag:

    telemetry_derived__firefox_nondesktop_day_2_7_activation__v1 = bigquery_etl_query(
        task_id="telemetry_derived__firefox_nondesktop_day_2_7_activation__v1",
        destination_table="firefox_nondesktop_day_2_7_activation_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="gkaberere@mozilla.com",
        email=["gkaberere@mozilla.com", "jklukas@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        dag=dag,
    )

    telemetry_derived__firefox_nondesktop_exact_mau28__v1 = bigquery_etl_query(
        task_id="telemetry_derived__firefox_nondesktop_exact_mau28__v1",
        destination_table="firefox_nondesktop_exact_mau28_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jklukas@mozilla.com",
        email=["jklukas@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        dag=dag,
    )

    firefox_nondesktop_exact_mau28_by_client_count_dimensions = bigquery_etl_query(
        task_id="firefox_nondesktop_exact_mau28_by_client_count_dimensions",
        destination_table="firefox_nondesktop_exact_mau28_by_client_count_dimensions_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jklukas@mozilla.com",
        email=["jklukas@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        dag=dag,
    )
