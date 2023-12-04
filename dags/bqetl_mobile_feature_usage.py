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

Built from bigquery-etl repo, [`dags/bqetl_mobile_feature_usage.py`](https://github.com/mozilla/bigquery-etl/blob/main/dags/bqetl_mobile_feature_usage.py)

#### Description

schedule run for mobile feature usage tables
#### Owner

rzhao@mozilla.com
"""


default_args = {
    "owner": "rzhao@mozilla.com",
    "start_date": datetime.datetime(2023, 10, 24, 0, 0),
    "end_date": None,
    "email": ["telemetry-alerts@mozilla.com", "rzhao@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
}

tags = ["impact/tier_3", "repo/bigquery-etl"]

with DAG(
    "bqetl_mobile_feature_usage",
    default_args=default_args,
    schedule_interval="0 4 * * *",
    doc_md=docs,
    tags=tags,
) as dag:
    fenix_feature_usage_events__v1 = bigquery_etl_query(
        task_id="fenix_feature_usage_events__v1",
        destination_table="feature_usage_events_v1",
        dataset_id="fenix_derived",
        project_id="moz-fx-data-shared-prod",
        owner="rzhao@mozilla.com",
        email=["rzhao@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    fenix_feature_usage_metrics__v1 = bigquery_etl_query(
        task_id="fenix_feature_usage_metrics__v1",
        destination_table="feature_usage_metrics_v1",
        dataset_id="fenix_derived",
        project_id="moz-fx-data-shared-prod",
        owner="rzhao@mozilla.com",
        email=["rzhao@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    ios_feature_usage_events__v1 = bigquery_etl_query(
        task_id="ios_feature_usage_events__v1",
        destination_table="feature_usage_events_v1",
        dataset_id="firefox_ios_derived",
        project_id="moz-fx-data-shared-prod",
        owner="rzhao@mozilla.com",
        email=["rzhao@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    ios_feature_usage_metrics__v1 = bigquery_etl_query(
        task_id="ios_feature_usage_metrics__v1",
        destination_table="feature_usage_metrics_v1",
        dataset_id="firefox_ios_derived",
        project_id="moz-fx-data-shared-prod",
        owner="rzhao@mozilla.com",
        email=["rzhao@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )
