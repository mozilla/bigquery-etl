# Generated via https://github.com/mozilla/bigquery-etl/blob/main/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskMarker
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
import datetime
from utils.constants import ALLOWED_STATES, FAILED_STATES
from utils.gcp import bigquery_etl_query, gke_command, bigquery_dq_check

docs = """
### bqetl_activity_stream

Built from bigquery-etl repo, [`dags/bqetl_activity_stream.py`](https://github.com/mozilla/bigquery-etl/blob/main/dags/bqetl_activity_stream.py)

#### Description

Daily aggregations on top of pings sent for the `activity_stream`
namespace by desktop Firefox. These are largely related to activity
on the newtab page and engagement with Pocket content.

#### Owner

anicholson@mozilla.com
"""


default_args = {
    "owner": "anicholson@mozilla.com",
    "start_date": datetime.datetime(2019, 7, 25, 0, 0),
    "end_date": None,
    "email": ["telemetry-alerts@mozilla.com", "anicholson@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=300),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
}

tags = ["impact/tier_2", "repo/bigquery-etl"]

with DAG(
    "bqetl_activity_stream",
    default_args=default_args,
    schedule_interval="0 2 * * *",
    doc_md=docs,
    tags=tags,
) as dag:
    activity_stream_bi__impression_stats_by_experiment__v1 = bigquery_etl_query(
        task_id="activity_stream_bi__impression_stats_by_experiment__v1",
        destination_table="impression_stats_by_experiment_v1",
        dataset_id="activity_stream_bi",
        project_id="moz-fx-data-shared-prod",
        owner="anicholson@mozilla.com",
        email=["anicholson@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    activity_stream_bi__impression_stats_flat__v1 = bigquery_etl_query(
        task_id="activity_stream_bi__impression_stats_flat__v1",
        destination_table="impression_stats_flat_v1",
        dataset_id="activity_stream_bi",
        project_id="moz-fx-data-shared-prod",
        owner="anicholson@mozilla.com",
        email=["anicholson@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    activity_stream_bi__impression_stats_by_experiment__v1.set_upstream(
        activity_stream_bi__impression_stats_flat__v1
    )

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

    activity_stream_bi__impression_stats_flat__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )
