# Generated via https://github.com/mozilla/bigquery-etl/blob/master/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from airflow.operators.sensors import ExternalTaskSensor
import datetime
from utils.gcp import bigquery_etl_query, gke_command

default_args = {
    "owner": "jklukas@mozilla.com",
    "start_date": datetime.datetime(2019, 3, 1, 0, 0),
    "end_date": None,
    "email": ["telemetry-alerts@mozilla.com", "jklukas@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=600),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
}

with DAG(
    "bqetl_fxa_events", default_args=default_args, schedule_interval="30 1 * * *"
) as dag:

    firefox_accounts_derived__exact_mau28__v1 = bigquery_etl_query(
        task_id="firefox_accounts_derived__exact_mau28__v1",
        destination_table="exact_mau28_v1",
        dataset_id="firefox_accounts_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jklukas@mozilla.com",
        email=["jklukas@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        dag=dag,
    )

    firefox_accounts_derived__fxa_auth_bounce_events__v1 = bigquery_etl_query(
        task_id="firefox_accounts_derived__fxa_auth_bounce_events__v1",
        destination_table="fxa_auth_bounce_events_v1",
        dataset_id="firefox_accounts_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jklukas@mozilla.com",
        email=["jklukas@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        arguments=["--schema_update_option=ALLOW_FIELD_ADDITION"],
        dag=dag,
    )

    firefox_accounts_derived__fxa_auth_events__v1 = bigquery_etl_query(
        task_id="firefox_accounts_derived__fxa_auth_events__v1",
        destination_table="fxa_auth_events_v1",
        dataset_id="firefox_accounts_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jklukas@mozilla.com",
        email=["jklukas@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        arguments=["--schema_update_option=ALLOW_FIELD_ADDITION"],
        dag=dag,
    )

    firefox_accounts_derived__fxa_content_events__v1 = bigquery_etl_query(
        task_id="firefox_accounts_derived__fxa_content_events__v1",
        destination_table="fxa_content_events_v1",
        dataset_id="firefox_accounts_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jklukas@mozilla.com",
        email=["jklukas@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        arguments=["--schema_update_option=ALLOW_FIELD_ADDITION"],
        dag=dag,
    )

    firefox_accounts_derived__fxa_delete_events__v1 = bigquery_etl_query(
        task_id="firefox_accounts_derived__fxa_delete_events__v1",
        destination_table="fxa_delete_events_v1",
        dataset_id="firefox_accounts_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jklukas@mozilla.com",
        email=["jklukas@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        dag=dag,
    )

    firefox_accounts_derived__fxa_log_auth_events__v1 = bigquery_etl_query(
        task_id="firefox_accounts_derived__fxa_log_auth_events__v1",
        destination_table="fxa_log_auth_events_v1",
        dataset_id="firefox_accounts_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jklukas@mozilla.com",
        email=["jklukas@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        dag=dag,
    )

    firefox_accounts_derived__fxa_log_content_events__v1 = bigquery_etl_query(
        task_id="firefox_accounts_derived__fxa_log_content_events__v1",
        destination_table="fxa_log_content_events_v1",
        dataset_id="firefox_accounts_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jklukas@mozilla.com",
        email=["jklukas@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        dag=dag,
    )

    firefox_accounts_derived__fxa_log_device_command_events__v1 = bigquery_etl_query(
        task_id="firefox_accounts_derived__fxa_log_device_command_events__v1",
        destination_table="fxa_log_device_command_events_v1",
        dataset_id="firefox_accounts_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jklukas@mozilla.com",
        email=["jklukas@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        dag=dag,
    )

    firefox_accounts_derived__fxa_users_daily__v1 = bigquery_etl_query(
        task_id="firefox_accounts_derived__fxa_users_daily__v1",
        destination_table="fxa_users_daily_v1",
        dataset_id="firefox_accounts_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jklukas@mozilla.com",
        email=["jklukas@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        dag=dag,
    )

    firefox_accounts_derived__fxa_users_last_seen__v1 = bigquery_etl_query(
        task_id="firefox_accounts_derived__fxa_users_last_seen__v1",
        destination_table="fxa_users_last_seen_v1",
        dataset_id="firefox_accounts_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jklukas@mozilla.com",
        email=["jklukas@mozilla.com", "telemetry-alerts@mozilla.com"],
        start_date=datetime.datetime(2019, 4, 23, 0, 0),
        date_partition_parameter="submission_date",
        depends_on_past=True,
        dag=dag,
    )

    firefox_accounts_derived__fxa_users_services_daily__v1 = bigquery_etl_query(
        task_id="firefox_accounts_derived__fxa_users_services_daily__v1",
        destination_table="fxa_users_services_daily_v1",
        dataset_id="firefox_accounts_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jklukas@mozilla.com",
        email=["jklukas@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        dag=dag,
    )

    firefox_accounts_derived__fxa_users_services_first_seen__v1 = bigquery_etl_query(
        task_id="firefox_accounts_derived__fxa_users_services_first_seen__v1",
        destination_table="fxa_users_services_first_seen_v1",
        dataset_id="firefox_accounts_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jklukas@mozilla.com",
        email=["jklukas@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        dag=dag,
    )

    firefox_accounts_derived__fxa_users_services_last_seen__v1 = bigquery_etl_query(
        task_id="firefox_accounts_derived__fxa_users_services_last_seen__v1",
        destination_table="fxa_users_services_last_seen_v1",
        dataset_id="firefox_accounts_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jklukas@mozilla.com",
        email=["jklukas@mozilla.com", "telemetry-alerts@mozilla.com"],
        start_date=datetime.datetime(2019, 10, 8, 0, 0),
        date_partition_parameter="submission_date",
        depends_on_past=True,
        dag=dag,
    )

    firefox_accounts_derived__exact_mau28__v1.set_upstream(
        firefox_accounts_derived__fxa_users_last_seen__v1
    )

    firefox_accounts_derived__fxa_users_daily__v1.set_upstream(
        firefox_accounts_derived__fxa_auth_bounce_events__v1
    )

    firefox_accounts_derived__fxa_users_daily__v1.set_upstream(
        firefox_accounts_derived__fxa_auth_events__v1
    )

    firefox_accounts_derived__fxa_users_daily__v1.set_upstream(
        firefox_accounts_derived__fxa_content_events__v1
    )

    firefox_accounts_derived__fxa_users_last_seen__v1.set_upstream(
        firefox_accounts_derived__fxa_users_daily__v1
    )

    firefox_accounts_derived__fxa_users_services_daily__v1.set_upstream(
        firefox_accounts_derived__fxa_auth_events__v1
    )

    firefox_accounts_derived__fxa_users_services_daily__v1.set_upstream(
        firefox_accounts_derived__fxa_content_events__v1
    )

    firefox_accounts_derived__fxa_users_services_first_seen__v1.set_upstream(
        firefox_accounts_derived__fxa_auth_events__v1
    )

    firefox_accounts_derived__fxa_users_services_first_seen__v1.set_upstream(
        firefox_accounts_derived__fxa_content_events__v1
    )

    firefox_accounts_derived__fxa_users_services_last_seen__v1.set_upstream(
        firefox_accounts_derived__fxa_users_services_daily__v1
    )

    firefox_accounts_derived__fxa_users_services_last_seen__v1.set_upstream(
        firefox_accounts_derived__fxa_users_services_first_seen__v1
    )
