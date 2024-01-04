# Generated via https://github.com/mozilla/bigquery-etl/blob/main/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskMarker
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
import datetime
from utils.constants import ALLOWED_STATES, FAILED_STATES
from utils.gcp import bigquery_etl_query, gke_command, bigquery_dq_check

docs = """
### bqetl_mobile_feature_usage

Built from bigquery-etl repo, [`dags/bqetl_mobile_feature_usage.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_mobile_feature_usage.py)

#### Description

Schedule run for mobile feature usage tables
#### Owner

rzhao@mozilla.com

#### Tags

* impact/tier_3
* repo/bigquery-etl
"""


default_args = {
    "owner": "rzhao@mozilla.com",
    "start_date": datetime.datetime(2023, 10, 24, 0, 0),
    "end_date": None,
    "email": ["telemetry-alerts@mozilla.com", "rzhao@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
}

tags = ["impact/tier_3", "repo/bigquery-etl"]

with DAG(
    "bqetl_mobile_feature_usage",
    default_args=default_args,
    schedule_interval="0 6 * * *",
    doc_md=docs,
    tags=tags,
) as dag:
    fenix_derived__feature_usage_events__v1 = bigquery_etl_query(
        task_id="fenix_derived__feature_usage_events__v1",
        destination_table="feature_usage_events_v1",
        dataset_id="fenix_derived",
        project_id="moz-fx-data-shared-prod",
        owner="rzhao@mozilla.com",
        email=["rzhao@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    fenix_derived__feature_usage_metrics__v1 = bigquery_etl_query(
        task_id="fenix_derived__feature_usage_metrics__v1",
        destination_table="feature_usage_metrics_v1",
        dataset_id="fenix_derived",
        project_id="moz-fx-data-shared-prod",
        owner="rzhao@mozilla.com",
        email=["rzhao@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    firefox_ios_derived__feature_usage_events__v1 = bigquery_etl_query(
        task_id="firefox_ios_derived__feature_usage_events__v1",
        destination_table="feature_usage_events_v1",
        dataset_id="firefox_ios_derived",
        project_id="moz-fx-data-shared-prod",
        owner="rzhao@mozilla.com",
        email=["rzhao@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    firefox_ios_derived__feature_usage_metrics__v1 = bigquery_etl_query(
        task_id="firefox_ios_derived__feature_usage_metrics__v1",
        destination_table="feature_usage_metrics_v1",
        dataset_id="firefox_ios_derived",
        project_id="moz-fx-data-shared-prod",
        owner="rzhao@mozilla.com",
        email=["rzhao@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    wait_for_copy_deduplicate_all = ExternalTaskSensor(
        task_id="wait_for_copy_deduplicate_all",
        external_dag_id="copy_deduplicate",
        external_task_id="copy_deduplicate_all",
        execution_delta=datetime.timedelta(seconds=18000),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    fenix_derived__feature_usage_events__v1.set_upstream(wait_for_copy_deduplicate_all)

    fenix_derived__feature_usage_metrics__v1.set_upstream(wait_for_copy_deduplicate_all)

    firefox_ios_derived__feature_usage_events__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    firefox_ios_derived__feature_usage_metrics__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )
