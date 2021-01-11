# Generated via https://github.com/mozilla/bigquery-etl/blob/master/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from airflow.operators.sensors import ExternalTaskSensor
import datetime
from utils.gcp import bigquery_etl_query, gke_command

default_args = {
    "owner": "jklukas@mozilla.com",
    "start_date": datetime.datetime(2020, 9, 17, 0, 0),
    "end_date": None,
    "email": ["jklukas@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
}

with DAG(
    "bqetl_account_ecosystem", default_args=default_args, schedule_interval="0 2 * * *"
) as dag:

    account_ecosystem_derived__desktop_clients_daily__v1 = bigquery_etl_query(
        task_id="account_ecosystem_derived__desktop_clients_daily__v1",
        destination_table="desktop_clients_daily_v1",
        dataset_id="account_ecosystem_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jklukas@mozilla.com",
        email=["jklukas@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        dag=dag,
    )

    account_ecosystem_derived__ecosystem_client_id_lookup__v1 = bigquery_etl_query(
        task_id="account_ecosystem_derived__ecosystem_client_id_lookup__v1",
        destination_table="ecosystem_client_id_lookup_v1",
        dataset_id="account_ecosystem_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jklukas@mozilla.com",
        email=["jklukas@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=True,
        dag=dag,
    )

    account_ecosystem_derived__ecosystem_user_id_lookup__v1 = bigquery_etl_query(
        task_id="account_ecosystem_derived__ecosystem_user_id_lookup__v1",
        destination_table=None,
        dataset_id="account_ecosystem_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jklukas@mozilla.com",
        email=["jklukas@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=True,
        parameters=["submission_date:DATE:{{ds}}"],
        sql_file_path="sql/moz-fx-data-shared-prod/account_ecosystem_derived/ecosystem_user_id_lookup_v1/script.sql",
        dag=dag,
    )

    account_ecosystem_derived__fxa_logging_users_daily__v1 = bigquery_etl_query(
        task_id="account_ecosystem_derived__fxa_logging_users_daily__v1",
        destination_table="fxa_logging_users_daily_v1",
        dataset_id="account_ecosystem_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jklukas@mozilla.com",
        email=["jklukas@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        dag=dag,
    )

    account_ecosystem_restricted__ecosystem_client_id_deletion__v1 = bigquery_etl_query(
        task_id="account_ecosystem_restricted__ecosystem_client_id_deletion__v1",
        destination_table="ecosystem_client_id_deletion_v1",
        dataset_id="account_ecosystem_restricted",
        project_id="moz-fx-data-shared-prod",
        owner="jklukas@mozilla.com",
        email=["jklukas@mozilla.com"],
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

    account_ecosystem_derived__desktop_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    account_ecosystem_derived__ecosystem_client_id_lookup__v1.set_upstream(
        account_ecosystem_derived__ecosystem_user_id_lookup__v1
    )
    account_ecosystem_derived__ecosystem_client_id_lookup__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    account_ecosystem_derived__ecosystem_user_id_lookup__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    account_ecosystem_derived__fxa_logging_users_daily__v1.set_upstream(
        account_ecosystem_derived__ecosystem_user_id_lookup__v1
    )
    account_ecosystem_derived__fxa_logging_users_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    account_ecosystem_restricted__ecosystem_client_id_deletion__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )
