# Generated via https://github.com/mozilla/bigquery-etl/blob/main/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskMarker
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
import datetime
from utils.constants import ALLOWED_STATES, FAILED_STATES
from utils.gcp import bigquery_etl_query, gke_command

docs = """
### bqetl_ctxsvc_derived

Built from bigquery-etl repo, [`dags/bqetl_ctxsvc_derived.py`](https://github.com/mozilla/bigquery-etl/blob/main/dags/bqetl_ctxsvc_derived.py)

#### Description

Contextual services derived tables
#### Owner

ctroy@mozilla.com
"""


default_args = {
    "owner": "ctroy@mozilla.com",
    "start_date": datetime.datetime(2021, 5, 1, 0, 0),
    "end_date": None,
    "email": [
        "ctroy@mozilla.com",
        "wstuckey@mozilla.com",
        "telemetry-alerts@mozilla.com",
    ],
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

    contextual_services_derived__event_aggregates_spons_tiles__v1 = bigquery_etl_query(
        task_id="contextual_services_derived__event_aggregates_spons_tiles__v1",
        destination_table="event_aggregates_spons_tiles_v1",
        dataset_id="contextual_services_derived",
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
    )

    contextual_services_derived__event_aggregates_suggest__v1 = bigquery_etl_query(
        task_id="contextual_services_derived__event_aggregates_suggest__v1",
        destination_table="event_aggregates_suggest_v1",
        dataset_id="contextual_services_derived",
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

    contextual_services_derived__event_aggregates__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    contextual_services_derived__event_aggregates_spons_tiles__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    contextual_services_derived__event_aggregates_suggest__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )
