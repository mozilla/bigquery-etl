# Generated via https://github.com/mozilla/bigquery-etl/blob/master/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from airflow.operators.sensors import ExternalTaskSensor
import datetime
from utils.gcp import bigquery_etl_query, gke_command

docs = """
### bqetl_mobile_search

Built from bigquery-etl repo, [`dags/bqetl_mobile_search.py`](https://github.com/mozilla/bigquery-etl/blob/master/dags/bqetl_mobile_search.py)

#### Owner

bewu@mozilla.com
"""


default_args = {
    "owner": "bewu@mozilla.com",
    "start_date": datetime.datetime(2019, 7, 25, 0, 0),
    "end_date": None,
    "email": ["telemetry-alerts@mozilla.com", "bewu@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=300),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
}

with DAG(
    "bqetl_mobile_search",
    default_args=default_args,
    schedule_interval="0 2 * * *",
    doc_md=docs,
) as dag:

    search_derived__mobile_search_aggregates__v1 = bigquery_etl_query(
        task_id="search_derived__mobile_search_aggregates__v1",
        destination_table="mobile_search_aggregates_v1",
        dataset_id="search_derived",
        project_id="moz-fx-data-shared-prod",
        owner="bewu@mozilla.com",
        email=["bewu@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        dag=dag,
    )

    search_derived__mobile_search_clients_daily__v1 = bigquery_etl_query(
        task_id="search_derived__mobile_search_clients_daily__v1",
        destination_table="mobile_search_clients_daily_v1",
        dataset_id="search_derived",
        project_id="moz-fx-data-shared-prod",
        owner="bewu@mozilla.com",
        email=["bewu@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        allow_field_addition_on_date="2020-07-13",
        dag=dag,
    )

    search_derived__mobile_search_clients_last_seen__v1 = bigquery_etl_query(
        task_id="search_derived__mobile_search_clients_last_seen__v1",
        destination_table="mobile_search_clients_last_seen_v1",
        dataset_id="search_derived",
        project_id="moz-fx-data-shared-prod",
        owner="bewu@mozilla.com",
        email=["bewu@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=True,
        dag=dag,
    )

    search_derived__mobile_search_aggregates__v1.set_upstream(
        search_derived__mobile_search_clients_daily__v1
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

    search_derived__mobile_search_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    search_derived__mobile_search_clients_last_seen__v1.set_upstream(
        search_derived__mobile_search_clients_daily__v1
    )
