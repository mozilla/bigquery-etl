# Generated via https://github.com/mozilla/bigquery-etl/blob/main/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from operators.task_sensor import ExternalTaskCompletedSensor
import datetime
from utils.gcp import bigquery_etl_query, gke_command

docs = """
### bqetl_search_terms_daily

Built from bigquery-etl repo, [`dags/bqetl_search_terms_daily.py`](https://github.com/mozilla/bigquery-etl/blob/main/dags/bqetl_search_terms_daily.py)

#### Description

Derived tables on top of search terms data.

#### Owner

jklukas@mozilla.com
"""


default_args = {
    "owner": "jklukas@mozilla.com",
    "start_date": datetime.datetime(2021, 9, 20, 0, 0),
    "end_date": None,
    "email": [
        "jklukas@mozilla.com",
        "rburwei@mozilla.com",
        "telemetry-alerts@mozilla.com",
    ],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
}

with DAG(
    "bqetl_search_terms_daily",
    default_args=default_args,
    schedule_interval="0 3 * * *",
    doc_md=docs,
) as dag:

    search_terms_derived__adm_weekly_aggregates__v1 = bigquery_etl_query(
        task_id="search_terms_derived__adm_weekly_aggregates__v1",
        destination_table="adm_weekly_aggregates_v1",
        dataset_id="search_terms_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jklukas@mozilla.com",
        email=[
            "jklukas@mozilla.com",
            "rburwei@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        dag=dag,
    )

    search_terms_derived__aggregated_search_terms_daily__v1 = bigquery_etl_query(
        task_id="search_terms_derived__aggregated_search_terms_daily__v1",
        destination_table="aggregated_search_terms_daily_v1",
        dataset_id="search_terms_derived",
        project_id="moz-fx-data-shared-prod",
        owner="rburwei@mozilla.com",
        email=[
            "jklukas@mozilla.com",
            "rburwei@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        dag=dag,
    )

    search_terms_derived__suggest_impression_sanitized__v1 = bigquery_etl_query(
        task_id="search_terms_derived__suggest_impression_sanitized__v1",
        destination_table="suggest_impression_sanitized_v1",
        dataset_id="search_terms_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jklukas@mozilla.com",
        email=[
            "jklukas@mozilla.com",
            "rburwei@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        dag=dag,
    )

    wait_for_copy_deduplicate_all = ExternalTaskCompletedSensor(
        task_id="wait_for_copy_deduplicate_all",
        external_dag_id="copy_deduplicate",
        external_task_id="copy_deduplicate_all",
        execution_delta=datetime.timedelta(seconds=7200),
        check_existence=True,
        mode="reschedule",
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    search_terms_derived__adm_weekly_aggregates__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    search_terms_derived__aggregated_search_terms_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    search_terms_derived__suggest_impression_sanitized__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )
