# Generated via https://github.com/mozilla/bigquery-etl/blob/main/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from operators.task_sensor import ExternalTaskCompletedSensor
import datetime
from utils.gcp import bigquery_etl_query, gke_command

docs = """
### bqetl_ctxsvc_derived

Built from bigquery-etl repo, [`dags/bqetl_ctxsvc_derived.py`](https://github.com/mozilla/bigquery-etl/blob/main/dags/bqetl_ctxsvc_derived.py)

#### Description

Contextual services derived tables
#### Owner

jklukas@mozilla.com
"""


default_args = {
    "owner": "jklukas@mozilla.com",
    "start_date": datetime.datetime(2021, 5, 1, 0, 0),
    "end_date": None,
    "email": ["jklukas@mozilla.com", "telemetry-alerts@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
}

tags = ["impact/tier_2", "repo/bigquery-etl"]

with DAG(
    "bqetl_ctxsvc_derived",
    default_args=default_args,
    schedule_interval="0 3 * * *",
    doc_md=docs,
    tags=tags,
) as dag:

    contextual_services_derived__event_aggregates__v1 = bigquery_etl_query(
        task_id="contextual_services_derived__event_aggregates__v1",
        destination_table="event_aggregates_v1",
        dataset_id="contextual_services_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jklukas@mozilla.com",
        email=["jklukas@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        arguments=["--schema_update_option=ALLOW_FIELD_ADDITION"],
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

    contextual_services_derived__event_aggregates__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )
    wait_for_search_terms_derived__suggest_impression_sanitized__v1 = (
        ExternalTaskCompletedSensor(
            task_id="wait_for_search_terms_derived__suggest_impression_sanitized__v1",
            external_dag_id="bqetl_search_terms_daily",
            external_task_id="search_terms_derived__suggest_impression_sanitized__v1",
            check_existence=True,
            mode="reschedule",
            pool="DATA_ENG_EXTERNALTASKSENSOR",
        )
    )

    contextual_services_derived__event_aggregates__v1.set_upstream(
        wait_for_search_terms_derived__suggest_impression_sanitized__v1
    )
