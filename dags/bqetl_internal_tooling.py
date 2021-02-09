# Generated via https://github.com/mozilla/bigquery-etl/blob/master/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from airflow.operators.sensors import ExternalTaskSensor
import datetime
from utils.gcp import bigquery_etl_query, gke_command

docs = """
### bqetl_internal_tooling

Built from bigquery-etl repo, [`dags/bqetl_internal_tooling.py`](https://github.com/mozilla/bigquery-etl/blob/master/dags/bqetl_internal_tooling.py)

#### Description

This DAG schedules queries for populating queries related to Mozilla's internal developer tooling (e.g. mozregression).

#### Owner

wlachance@mozilla.com
"""


default_args = {
    "owner": "wlachance@mozilla.com",
    "start_date": datetime.datetime(2020, 6, 1, 0, 0),
    "end_date": None,
    "email": ["wlachance@mozilla.com", "telemetry-alerts@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
}

with DAG(
    "bqetl_internal_tooling",
    default_args=default_args,
    schedule_interval="0 4 * * *",
    doc_md=docs,
) as dag:

    mozregression_aggregates__v1 = bigquery_etl_query(
        task_id="mozregression_aggregates__v1",
        destination_table="mozregression_aggregates_v1",
        dataset_id="org_mozilla_mozregression_derived",
        project_id="moz-fx-data-shared-prod",
        owner="wlachance@mozilla.com",
        email=["telemetry-alerts@mozilla.com", "wlachance@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        dag=dag,
    )

    wait_for_copy_deduplicate_all = ExternalTaskSensor(
        task_id="wait_for_copy_deduplicate_all",
        external_dag_id="copy_deduplicate",
        external_task_id="copy_deduplicate_all",
        execution_delta=datetime.timedelta(seconds=10800),
        check_existence=True,
        mode="reschedule",
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    mozregression_aggregates__v1.set_upstream(wait_for_copy_deduplicate_all)
