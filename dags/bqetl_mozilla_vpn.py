# Generated via https://github.com/mozilla/bigquery-etl/blob/master/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from airflow.operators.sensors import ExternalTaskSensor
import datetime
from utils.gcp import bigquery_etl_query

default_args = {
    "owner": "dthorn@mozilla.com",
    "start_date": datetime.datetime(2020, 10, 8, 0, 0),
    "email": ["telemetry-alerts@mozilla.com", "dthorn@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
}

with DAG(
    "bqetl_mozilla_vpn", default_args=default_args, schedule_interval="@daily"
) as dag:

    mozilla_vpn_derived__waitlist__v1 = bigquery_etl_query(
        task_id="mozilla_vpn_derived__waitlist__v1",
        destination_table="waitlist_v1",
        dataset_id="mozilla_vpn_derived",
        project_id="moz-fx-data-shared-prod",
        owner="dthorn@mozilla.com",
        email=["dthorn@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        dag=dag,
    )

    mozilla_vpn_derived__waitlist_funnel__v1 = bigquery_etl_query(
        task_id="mozilla_vpn_derived__waitlist_funnel__v1",
        destination_table="waitlist_funnel_v1",
        dataset_id="mozilla_vpn_derived",
        project_id="moz-fx-data-shared-prod",
        owner="dthorn@mozilla.com",
        email=["dthorn@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        dag=dag,
    )

    mozilla_vpn_external__devices__v1 = bigquery_etl_query(
        task_id="mozilla_vpn_external__devices__v1",
        destination_table="devices_v1",
        dataset_id="mozilla_vpn_external",
        project_id="moz-fx-data-shared-prod",
        owner="dthorn@mozilla.com",
        email=["dthorn@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=True,
        parameters=[
            "external_database_query:STRING:SELECT * FROM devices WHERE DATE(updated_at) = DATE '{{ds}}'"
        ],
        dag=dag,
    )

    mozilla_vpn_external__users__v1 = bigquery_etl_query(
        task_id="mozilla_vpn_external__users__v1",
        destination_table="users_v1",
        dataset_id="mozilla_vpn_external",
        project_id="moz-fx-data-shared-prod",
        owner="dthorn@mozilla.com",
        email=["dthorn@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=True,
        parameters=[
            "external_database_query:STRING:SELECT * FROM users WHERE DATE(updated_at) = DATE '{{ds}}'"
        ],
        dag=dag,
    )

    mozilla_vpn_external__waitlist__v1 = bigquery_etl_query(
        task_id="mozilla_vpn_external__waitlist__v1",
        destination_table="waitlist_v1",
        dataset_id="mozilla_vpn_external",
        project_id="moz-fx-data-shared-prod",
        owner="dthorn@mozilla.com",
        email=["dthorn@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=True,
        parameters=[
            "external_database_query:STRING:SELECT * FROM vpn_waitlist WHERE DATE(updated_at) = DATE '{{ds}}'"
        ],
        dag=dag,
    )

    mozilla_vpn_derived__waitlist__v1.set_upstream(mozilla_vpn_external__waitlist__v1)

    mozilla_vpn_derived__waitlist_funnel__v1.set_upstream(
        mozilla_vpn_external__users__v1
    )
