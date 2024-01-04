# Generated via https://github.com/mozilla/bigquery-etl/blob/main/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskMarker
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
import datetime
from utils.constants import ALLOWED_STATES, FAILED_STATES
from utils.gcp import bigquery_etl_query, gke_command, bigquery_dq_check

docs = """
### bqetl_fenix_event_rollup

Built from bigquery-etl repo, [`dags/bqetl_fenix_event_rollup.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_fenix_event_rollup.py)

#### Owner

wlachance@mozilla.com

#### Tags

* impact/tier_1
* repo/bigquery-etl
"""


default_args = {
    "owner": "wlachance@mozilla.com",
    "start_date": datetime.datetime(2020, 9, 9, 0, 0),
    "end_date": None,
    "email": ["wlachance@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
}

tags = ["impact/tier_1", "repo/bigquery-etl"]

with DAG(
    "bqetl_fenix_event_rollup",
    default_args=default_args,
    schedule_interval="0 2 * * *",
    doc_md=docs,
    tags=tags,
) as dag:
    fenix_derived__event_types__v1 = bigquery_etl_query(
        task_id="fenix_derived__event_types__v1",
        destination_table="event_types_v1",
        dataset_id="fenix_derived",
        project_id="moz-fx-data-shared-prod",
        owner="wlachance@mozilla.com",
        email=["akomar@mozilla.com", "wlachance@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
        parameters=["submission_date:DATE:{{ds}}"],
    )

    fenix_derived__event_types_history__v1 = bigquery_etl_query(
        task_id="fenix_derived__event_types_history__v1",
        destination_table="event_types_history_v1",
        dataset_id="fenix_derived",
        project_id="moz-fx-data-shared-prod",
        owner="wlachance@mozilla.com",
        email=["akomar@mozilla.com", "wlachance@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=True,
    )

    fenix_derived__events_daily__v1 = bigquery_etl_query(
        task_id="fenix_derived__events_daily__v1",
        destination_table="events_daily_v1",
        dataset_id="fenix_derived",
        project_id="moz-fx-data-shared-prod",
        owner="wlachance@mozilla.com",
        email=["akomar@mozilla.com", "wlachance@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    fenix_derived__event_types__v1.set_upstream(fenix_derived__event_types_history__v1)

    wait_for_copy_deduplicate_all = ExternalTaskSensor(
        task_id="wait_for_copy_deduplicate_all",
        external_dag_id="copy_deduplicate",
        external_task_id="copy_deduplicate_all",
        execution_delta=datetime.timedelta(seconds=3600),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    fenix_derived__event_types_history__v1.set_upstream(wait_for_copy_deduplicate_all)

    fenix_derived__events_daily__v1.set_upstream(fenix_derived__event_types__v1)
