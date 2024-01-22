# Generated via https://github.com/mozilla/bigquery-etl/blob/main/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskMarker
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
import datetime
from utils.constants import ALLOWED_STATES, FAILED_STATES
from utils.gcp import bigquery_etl_query, gke_command, bigquery_dq_check

docs = """
### bqetl_search_terms_daily

Built from bigquery-etl repo, [`dags/bqetl_search_terms_daily.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_search_terms_daily.py)

#### Description

Derived tables on top of search terms data.

Note that the tasks for populating `suggest_impression_sanitized_v*` are
particularly important because the source unsanitized dataset has only
a 2-day retention period, so errors fairly quickly become unrecoverable
and can impact reporting to partners. If this task errors out, it could
indicate trouble with an upstream task that runs in a restricted project
outside of Airflow. Contact `ctroy`, `wstuckey`, `whd`, and `jbuck`.

#### Owner

ctroy@mozilla.com

#### Tags

* impact/tier_1
* repo/bigquery-etl
"""


default_args = {
    "owner": "ctroy@mozilla.com",
    "start_date": datetime.datetime(2021, 9, 20, 0, 0),
    "end_date": None,
    "email": [
        "ctroy@mozilla.com",
        "wstuckey@mozilla.com",
        "rburwei@mozilla.com",
        "telemetry-alerts@mozilla.com",
    ],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
}

tags = ["impact/tier_1", "repo/bigquery-etl"]

with DAG(
    "bqetl_search_terms_daily",
    default_args=default_args,
    schedule_interval="0 3 * * *",
    doc_md=docs,
    tags=tags,
) as dag:
    search_terms_derived__adm_daily_aggregates__v1 = bigquery_etl_query(
        task_id="search_terms_derived__adm_daily_aggregates__v1",
        destination_table="adm_daily_aggregates_v1",
        dataset_id="search_terms_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ctroy@mozilla.com",
        email=[
            "ctroy@mozilla.com",
            "rburwei@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wstuckey@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        arguments=["--schema_update_option=ALLOW_FIELD_ADDITION"],
    )

    search_terms_derived__aggregated_search_terms_daily__v1 = bigquery_etl_query(
        task_id="search_terms_derived__aggregated_search_terms_daily__v1",
        destination_table="aggregated_search_terms_daily_v1",
        dataset_id="search_terms_derived",
        project_id="moz-fx-data-shared-prod",
        owner="rburwei@mozilla.com",
        email=[
            "ctroy@mozilla.com",
            "rburwei@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wstuckey@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        arguments=["--schema_update_option=ALLOW_FIELD_ADDITION"],
    )

    search_terms_derived__search_terms_daily__v1 = bigquery_etl_query(
        task_id="search_terms_derived__search_terms_daily__v1",
        destination_table="search_terms_daily_v1",
        dataset_id="search_terms_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ctroy@mozilla.com",
        email=[
            "ctroy@mozilla.com",
            "rburwei@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wstuckey@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        arguments=["--schema_update_option=ALLOW_FIELD_ADDITION"],
    )

    search_terms_derived__suggest_impression_sanitized__v2 = bigquery_etl_query(
        task_id="search_terms_derived__suggest_impression_sanitized__v2",
        destination_table="suggest_impression_sanitized_v2",
        dataset_id="search_terms_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ctroy@mozilla.com",
        email=[
            "ctroy@mozilla.com",
            "rburwei@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wstuckey@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        arguments=["--schema_update_option=ALLOW_FIELD_ADDITION"],
    )

    search_terms_derived__suggest_impression_sanitized__v3 = bigquery_etl_query(
        task_id="search_terms_derived__suggest_impression_sanitized__v3",
        destination_table="suggest_impression_sanitized_v3",
        dataset_id="search_terms_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ctroy@mozilla.com",
        email=[
            "ctroy@mozilla.com",
            "rburwei@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wstuckey@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        arguments=["--schema_update_option=ALLOW_FIELD_ADDITION"],
    )

    search_terms_derived__adm_daily_aggregates__v1.set_upstream(
        search_terms_derived__suggest_impression_sanitized__v2
    )

    search_terms_derived__aggregated_search_terms_daily__v1.set_upstream(
        search_terms_derived__suggest_impression_sanitized__v2
    )

    wait_for_copy_deduplicate_all = ExternalTaskSensor(
        task_id="wait_for_copy_deduplicate_all",
        external_dag_id="copy_deduplicate",
        external_task_id="copy_deduplicate_all",
        execution_delta=datetime.timedelta(seconds=7200),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    search_terms_derived__suggest_impression_sanitized__v3.set_upstream(
        wait_for_copy_deduplicate_all
    )
