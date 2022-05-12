# Generated via https://github.com/mozilla/bigquery-etl/blob/main/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from operators.task_sensor import ExternalTaskCompletedSensor
import datetime
from utils.gcp import bigquery_etl_query, gke_command

docs = """
### bqetl_analytics_aggregations

Built from bigquery-etl repo, [`dags/bqetl_analytics_aggregations.py`](https://github.com/mozilla/bigquery-etl/blob/main/dags/bqetl_analytics_aggregations.py)

#### Description

Scheduler to populate the aggregations required for analytics engineering and reports optimization. It provides data to build growth, search and usage metrics, as well as acquisition and retention KPIs, in a model that facilitates reporting in Looker.
#### Owner

lvargas@mozilla.com
"""


default_args = {
    "owner": "lvargas@mozilla.com",
    "start_date": datetime.datetime(2022, 5, 11, 0, 0),
    "end_date": None,
    "email": [
        "telemetry-alerts@mozilla.com",
        "lvargas@mozilla.com",
        "gkaberere@mozilla.com",
    ],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
}

tags = ["impact/tier_1", "repo/bigquery-etl"]

with DAG(
    "bqetl_analytics_aggregations",
    default_args=default_args,
    schedule_interval="0 1 * * *",
    doc_md=docs,
    tags=tags,
) as dag:

    active_users_aggregates_v1 = bigquery_etl_query(
        task_id="active_users_aggregates_v1",
        destination_table="active_users_aggregates_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="lvargas@mozilla.com",
        email=[
            "gkaberere@mozilla.com",
            "lvargas@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    agg_active_users = bigquery_etl_query(
        task_id="agg_active_users",
        destination_table="agg_active_users_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="lvargas@mozilla.com",
        email=[
            "gkaberere@mozilla.com",
            "lvargas@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    wait_for_telemetry_derived__unified_metrics__v1 = ExternalTaskCompletedSensor(
        task_id="wait_for_telemetry_derived__unified_metrics__v1",
        external_dag_id="bqetl_unified",
        external_task_id="telemetry_derived__unified_metrics__v1",
        execution_delta=datetime.timedelta(days=-1, seconds=79200),
        check_existence=True,
        mode="reschedule",
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    active_users_aggregates_v1.set_upstream(
        wait_for_telemetry_derived__unified_metrics__v1
    )

    agg_active_users.set_upstream(wait_for_telemetry_derived__unified_metrics__v1)
