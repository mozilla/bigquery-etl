# Generated via https://github.com/mozilla/bigquery-etl/blob/main/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskMarker
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
import datetime
from operators.gcp_container_operator import GKEPodOperator
from utils.constants import ALLOWED_STATES, FAILED_STATES
from utils.gcp import bigquery_etl_query, bigquery_dq_check, bigquery_bigeye_check

docs = """
### bqetl_default_browser_aggregates

Built from bigquery-etl repo, [`dags/bqetl_default_browser_aggregates.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_default_browser_aggregates.py)

#### Description

This DAG builds daily aggregate tables for default browser tables

#### Owner

wichan@mozilla.com

#### Tags

* impact/tier_3
* repo/bigquery-etl
"""


default_args = {
    "owner": "wichan@mozilla.com",
    "start_date": datetime.datetime(2024, 12, 20, 0, 0),
    "end_date": None,
    "email": ["telemetry-alerts@mozilla.com", "wichan@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
}

tags = ["impact/tier_3", "repo/bigquery-etl"]

with DAG(
    "bqetl_default_browser_aggregates",
    default_args=default_args,
    schedule_interval="0 22 * * *",
    doc_md=docs,
    tags=tags,
) as dag:

    wait_for_copy_deduplicate_all = ExternalTaskSensor(
        task_id="wait_for_copy_deduplicate_all",
        external_dag_id="copy_deduplicate",
        external_task_id="copy_deduplicate_all",
        execution_delta=datetime.timedelta(seconds=75600),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    default_browser_agent_derived__default_browser_agg__v1 = bigquery_etl_query(
        task_id="default_browser_agent_derived__default_browser_agg__v1",
        destination_table="default_browser_agg_v1",
        dataset_id="default_browser_agent_derived",
        project_id="moz-fx-data-shared-prod",
        owner="wichan@mozilla.com",
        email=["telemetry-alerts@mozilla.com", "wichan@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    default_browser_agent_derived__default_browser_agg_by_os__v1 = bigquery_etl_query(
        task_id="default_browser_agent_derived__default_browser_agg_by_os__v1",
        destination_table="default_browser_agg_by_os_v1",
        dataset_id="default_browser_agent_derived",
        project_id="moz-fx-data-shared-prod",
        owner="wichan@mozilla.com",
        email=["telemetry-alerts@mozilla.com", "wichan@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    default_browser_agent_derived__default_browser_agg__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    default_browser_agent_derived__default_browser_agg_by_os__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )
