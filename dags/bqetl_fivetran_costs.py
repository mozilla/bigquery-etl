# Generated via https://github.com/mozilla/bigquery-etl/blob/main/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskMarker
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
import datetime
from utils.constants import ALLOWED_STATES, FAILED_STATES
from utils.gcp import bigquery_etl_query, gke_command, bigquery_dq_check

from fivetran_provider.operators.fivetran import FivetranOperator
from fivetran_provider.sensors.fivetran import FivetranSensor
from utils.callbacks import retry_tasks_callback

docs = """
### bqetl_fivetran_costs

Built from bigquery-etl repo, [`dags/bqetl_fivetran_costs.py`](https://github.com/mozilla/bigquery-etl/blob/main/dags/bqetl_fivetran_costs.py)

#### Description

Derived tables for analyzing the Fivetran Costs. Data coming from Fivetran.

#### Owner

lschiestl@mozilla.com
"""


default_args = {
    "owner": "lschiestl@mozilla.com",
    "start_date": datetime.datetime(2023, 1, 18, 0, 0),
    "end_date": None,
    "email": ["telemetry-alerts@mozilla.com", "lschiestl@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
}

tags = ["impact/tier_3", "repo/bigquery-etl"]

with DAG(
    "bqetl_fivetran_costs",
    default_args=default_args,
    schedule_interval="0 5 * * *",
    doc_md=docs,
    tags=tags,
) as dag:
    checks__fivetran_costs_derived__daily_connector_costs__v1 = bigquery_dq_check(
        task_id="checks__fivetran_costs_derived__daily_connector_costs__v1",
        source_table="daily_connector_costs_v1",
        dataset_id="fivetran_costs_derived",
        project_id="moz-fx-data-shared-prod",
        owner="lschiestl@mozilla.com",
        email=["lschiestl@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        task_concurrency=1,
    )

    checks__fivetran_costs_derived__incremental_mar__v1 = bigquery_dq_check(
        task_id="checks__fivetran_costs_derived__incremental_mar__v1",
        source_table="incremental_mar_v1",
        dataset_id="fivetran_costs_derived",
        project_id="moz-fx-data-shared-prod",
        owner="lschiestl@mozilla.com",
        email=["lschiestl@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        task_concurrency=1,
    )

    checks__fivetran_costs_derived__monthly_costs__v1 = bigquery_dq_check(
        task_id="checks__fivetran_costs_derived__monthly_costs__v1",
        source_table="monthly_costs_v1",
        dataset_id="fivetran_costs_derived",
        project_id="moz-fx-data-shared-prod",
        owner="lschiestl@mozilla.com",
        email=["lschiestl@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        task_concurrency=1,
    )

    fivetran_costs_derived__daily_connector_costs__v1 = bigquery_etl_query(
        task_id="fivetran_costs_derived__daily_connector_costs__v1",
        destination_table="daily_connector_costs_v1",
        dataset_id="fivetran_costs_derived",
        project_id="moz-fx-data-shared-prod",
        owner="lschiestl@mozilla.com",
        email=["lschiestl@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    fivetran_costs_derived__destinations__v1 = bigquery_etl_query(
        task_id="fivetran_costs_derived__destinations__v1",
        destination_table="destinations_v1",
        dataset_id="fivetran_costs_derived",
        project_id="moz-fx-data-shared-prod",
        owner="lschiestl@mozilla.com",
        email=["lschiestl@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    fivetran_costs_derived__incremental_mar__v1 = bigquery_etl_query(
        task_id="fivetran_costs_derived__incremental_mar__v1",
        destination_table="incremental_mar_v1",
        dataset_id="fivetran_costs_derived",
        project_id="moz-fx-data-shared-prod",
        owner="lschiestl@mozilla.com",
        email=["lschiestl@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    fivetran_costs_derived__monthly_costs__v1 = bigquery_etl_query(
        task_id="fivetran_costs_derived__monthly_costs__v1",
        destination_table="monthly_costs_v1",
        dataset_id="fivetran_costs_derived",
        project_id="moz-fx-data-shared-prod",
        owner="lschiestl@mozilla.com",
        email=["lschiestl@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    checks__fivetran_costs_derived__daily_connector_costs__v1.set_upstream(
        fivetran_costs_derived__daily_connector_costs__v1
    )

    checks__fivetran_costs_derived__incremental_mar__v1.set_upstream(
        fivetran_costs_derived__incremental_mar__v1
    )

    checks__fivetran_costs_derived__monthly_costs__v1.set_upstream(
        fivetran_costs_derived__monthly_costs__v1
    )

    fivetran_costs_derived__daily_connector_costs__v1.set_upstream(
        checks__fivetran_costs_derived__incremental_mar__v1
    )

    fivetran_costs_derived__daily_connector_costs__v1.set_upstream(
        checks__fivetran_costs_derived__monthly_costs__v1
    )

    fivetran_costs_derived__daily_connector_costs__v1.set_upstream(
        fivetran_costs_derived__destinations__v1
    )

    fivetran_log_prod_sync_start = FivetranOperator(
        connector_id="{{ var.value.fivetran_log_prod_connector_id }}",
        task_id="fivetran_log_prod_task",
    )

    fivetran_log_prod_sync_wait = FivetranSensor(
        connector_id="{{ var.value.fivetran_log_prod_connector_id }}",
        task_id="fivetran_log_prod_sensor",
        poke_interval=30,
        xcom="{{ task_instance.xcom_pull('fivetran_log_prod_task') }}",
        on_retry_callback=retry_tasks_callback,
        params={"retry_tasks": ["fivetran_log_prod_task"]},
    )

    fivetran_log_prod_sync_wait.set_upstream(fivetran_log_prod_sync_start)

    fivetran_costs_derived__destinations__v1.set_upstream(fivetran_log_prod_sync_wait)

    fivetran_costs_derived__incremental_mar__v1.set_upstream(
        fivetran_log_prod_sync_wait
    )

    fivetran_costs_derived__monthly_costs__v1.set_upstream(fivetran_log_prod_sync_wait)
