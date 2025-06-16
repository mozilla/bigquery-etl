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
### bqetl_cohort_churn

Built from bigquery-etl repo, [`dags/bqetl_cohort_churn.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_cohort_churn.py)

#### Description

Schedules weekly churn query
#### Owner

kwindau@mozilla.com

#### Tags

* impact/tier_2
* repo/bigquery-etl
"""


default_args = {
    "owner": "kwindau@mozilla.com",
    "start_date": datetime.datetime(2025, 6, 15, 0, 0),
    "end_date": None,
    "email": ["kwindau@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "max_active_tis_per_dag": None,
}

tags = ["impact/tier_2", "repo/bigquery-etl"]

with DAG(
    "bqetl_cohort_churn",
    default_args=default_args,
    schedule_interval="20 20 * * 3",
    doc_md=docs,
    tags=tags,
    catchup=False,
) as dag:

    wait_for_telemetry_derived__cohort_weekly_active_clients_staging__v1 = ExternalTaskSensor(
        task_id="wait_for_telemetry_derived__cohort_weekly_active_clients_staging__v1",
        external_dag_id="bqetl_cohort_retention",
        external_task_id="telemetry_derived__cohort_weekly_active_clients_staging__v1",
        execution_delta=datetime.timedelta(seconds=2400),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_telemetry_derived__cohort_weekly_cfs_staging__v1 = ExternalTaskSensor(
        task_id="wait_for_telemetry_derived__cohort_weekly_cfs_staging__v1",
        external_dag_id="bqetl_cohort_retention",
        external_task_id="telemetry_derived__cohort_weekly_cfs_staging__v1",
        execution_delta=datetime.timedelta(seconds=2400),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    telemetry_derived__weekly_cohort_churn__v1 = bigquery_etl_query(
        task_id="telemetry_derived__weekly_cohort_churn__v1",
        destination_table="weekly_cohort_churn_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    telemetry_derived__weekly_cohort_churn__v1.set_upstream(
        wait_for_telemetry_derived__cohort_weekly_active_clients_staging__v1
    )

    telemetry_derived__weekly_cohort_churn__v1.set_upstream(
        wait_for_telemetry_derived__cohort_weekly_cfs_staging__v1
    )
