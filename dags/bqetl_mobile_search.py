# Generated via https://github.com/mozilla/bigquery-etl/blob/main/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from operators.task_sensor import ExternalTaskCompletedSensor
import datetime
from utils.gcp import bigquery_etl_query, gke_command

docs = """
### bqetl_mobile_search

Built from bigquery-etl repo, [`dags/bqetl_mobile_search.py`](https://github.com/mozilla/bigquery-etl/blob/main/dags/bqetl_mobile_search.py)

#### Owner

anicholson@mozilla.com
"""


default_args = {
    "owner": "anicholson@mozilla.com",
    "start_date": datetime.datetime(2019, 7, 25, 0, 0),
    "end_date": None,
    "email": [
        "telemetry-alerts@mozilla.com",
        "anicholson@mozilla.com",
        "akomar@mozilla.com",
    ],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=300),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
}

tags = ["impact/tier_1", "repo/bigquery-etl"]

with DAG(
    "bqetl_mobile_search",
    default_args=default_args,
    schedule_interval="0 2 * * *",
    doc_md=docs,
    tags=tags,
) as dag:

    search_derived__mobile_search_aggregates__v1 = bigquery_etl_query(
        task_id="search_derived__mobile_search_aggregates__v1",
        destination_table="mobile_search_aggregates_v1",
        dataset_id="search_derived",
        project_id="moz-fx-data-shared-prod",
        owner="akomar@mozilla.com",
        email=[
            "akomar@mozilla.com",
            "anicholson@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        dag=dag,
    )

    search_derived__mobile_search_clients_daily__v1 = bigquery_etl_query(
        task_id="search_derived__mobile_search_clients_daily__v1",
        destination_table="mobile_search_clients_daily_v1",
        dataset_id="search_derived",
        project_id="moz-fx-data-shared-prod",
        owner="akomar@mozilla.com",
        email=[
            "akomar@mozilla.com",
            "anicholson@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        dag=dag,
    )

    search_derived__mobile_search_clients_last_seen__v1 = bigquery_etl_query(
        task_id="search_derived__mobile_search_clients_last_seen__v1",
        destination_table="mobile_search_clients_last_seen_v1",
        dataset_id="search_derived",
        project_id="moz-fx-data-shared-prod",
        owner="akomar@mozilla.com",
        email=[
            "akomar@mozilla.com",
            "anicholson@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=True,
        dag=dag,
    )

    search_derived__mobile_search_aggregates__v1.set_upstream(
        search_derived__mobile_search_clients_daily__v1
    )

    wait_for_copy_deduplicate_all = ExternalTaskCompletedSensor(
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
