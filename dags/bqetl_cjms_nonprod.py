# Generated via https://github.com/mozilla/bigquery-etl/blob/main/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskMarker
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
import datetime
from utils.constants import ALLOWED_STATES, FAILED_STATES
from utils.gcp import bigquery_etl_query, gke_command

from fivetran_provider.operators.fivetran import FivetranOperator
from fivetran_provider.sensors.fivetran import FivetranSensor
from utils.callbacks import retry_tasks_callback

docs = """
### bqetl_cjms_nonprod

Built from bigquery-etl repo, [`dags/bqetl_cjms_nonprod.py`](https://github.com/mozilla/bigquery-etl/blob/main/dags/bqetl_cjms_nonprod.py)

#### Description

Hourly ETL for cjms nonprod.

#### Owner

srose@mozilla.com
"""


default_args = {
    "owner": "srose@mozilla.com",
    "start_date": datetime.datetime(2022, 3, 24, 0, 0),
    "end_date": None,
    "email": ["telemetry-alerts@mozilla.com", "srose@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=300),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
}

tags = ["impact/tier_3", "repo/bigquery-etl"]

with DAG(
    "bqetl_cjms_nonprod",
    default_args=default_args,
    schedule_interval="0 * * * *",
    doc_md=docs,
    tags=tags,
) as dag:
    cjms_bigquery__flows__v1 = bigquery_etl_query(
        task_id="cjms_bigquery__flows__v1",
        destination_table='flows_v1${{ (execution_date - macros.timedelta(hours=2)).strftime("%Y%m%d") }}',
        dataset_id="moz-fx-cjms-nonprod-9a36:cjms_bigquery",
        project_id="moz-fx-data-shared-prod",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        parameters=[
            'submission_date:DATE:{{ (execution_date - macros.timedelta(hours=2)).strftime("%Y-%m-%d") }}'
        ],
        sql_file_path="sql/moz-fx-cjms-nonprod-9a36/cjms_bigquery/flows_v1/query.sql",
    )

    cjms_bigquery__refunds__v1 = bigquery_etl_query(
        task_id="cjms_bigquery__refunds__v1",
        destination_table="refunds_v1",
        dataset_id="moz-fx-cjms-nonprod-9a36:cjms_bigquery",
        project_id="moz-fx-data-shared-prod",
        sql_file_path="sql/moz-fx-cjms-nonprod-9a36/cjms_bigquery/refunds_v1/query.sql",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    cjms_bigquery__subscriptions__v1 = bigquery_etl_query(
        task_id="cjms_bigquery__subscriptions__v1",
        destination_table="subscriptions_v1",
        dataset_id="moz-fx-cjms-nonprod-9a36:cjms_bigquery",
        project_id="moz-fx-data-shared-prod",
        sql_file_path="sql/moz-fx-cjms-nonprod-9a36/cjms_bigquery/subscriptions_v1/query.sql",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    fivetran_stripe_nonprod_sync_start = FivetranOperator(
        connector_id="{{ var.value.fivetran_stripe_nonprod_connector_id }}",
        task_id="fivetran_stripe_nonprod_task",
    )

    fivetran_stripe_nonprod_sync_wait = FivetranSensor(
        connector_id="{{ var.value.fivetran_stripe_nonprod_connector_id }}",
        task_id="fivetran_stripe_nonprod_sensor",
        poke_interval=30,
        xcom="{{ task_instance.xcom_pull('fivetran_stripe_nonprod_task') }}",
        on_retry_callback=retry_tasks_callback,
        params={"retry_tasks": ["fivetran_stripe_nonprod_task"]},
    )

    fivetran_stripe_nonprod_sync_wait.set_upstream(fivetran_stripe_nonprod_sync_start)

    cjms_bigquery__refunds__v1.set_upstream(fivetran_stripe_nonprod_sync_wait)

    cjms_bigquery__subscriptions__v1.set_upstream(cjms_bigquery__flows__v1)
    cjms_bigquery__subscriptions__v1.set_upstream(fivetran_stripe_nonprod_sync_wait)
