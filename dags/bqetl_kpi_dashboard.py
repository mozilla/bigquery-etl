# Generated via https://github.com/mozilla/bigquery-etl/blob/master/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from airflow.operators.sensors import ExternalTaskSensor
import datetime
from utils.gcp import bigquery_etl_query

default_args = {
    "owner": "jklukas@mozilla.com",
    "start_date": datetime.datetime(2020, 5, 12, 0, 0),
    "email": ["telemetry-alerts@mozilla.com", "jklukas@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=600),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
}

with DAG(
    "bqetl_kpi_dashboard", default_args=default_args, schedule_interval="45 15 * * *"
) as dag:

    telemetry_derived__smoot_usage_new_profiles__v2 = bigquery_etl_query(
        task_id="telemetry_derived__smoot_usage_new_profiles__v2",
        destination_table="smoot_usage_new_profiles_v2",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jklukas@mozilla.com",
        email=["jklukas@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        dag=dag,
    )

    telemetry_derived__smoot_usage_new_profiles_compressed__v2 = bigquery_etl_query(
        task_id="telemetry_derived__smoot_usage_new_profiles_compressed__v2",
        destination_table="smoot_usage_new_profiles_compressed_v2",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jklukas@mozilla.com",
        email=["jklukas@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        dag=dag,
    )

    kpi_dashboard = bigquery_etl_query(
        task_id="kpi_dashboard",
        destination_table="firefox_kpi_dashboard_v1",
        dataset_id="telemetry",
        project_id="moz-fx-data-shared-prod",
        owner="jklukas@mozilla.com",
        email=["jklukas@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        dag=dag,
    )

    telemetry_derived__smoot_usage_new_profiles_compressed__v2.set_upstream(
        telemetry_derived__smoot_usage_new_profiles__v2
    )
