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
### bqetl_sponsored_tiles_clients_daily

Built from bigquery-etl repo, [`dags/bqetl_sponsored_tiles_clients_daily.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_sponsored_tiles_clients_daily.py)

#### Description

daily run of sponsored tiles related fields
#### Owner

skahmann@mozilla.com

#### Tags

* impact/tier_3
* repo/bigquery-etl
"""


default_args = {
    "owner": "skahmann@mozilla.com",
    "start_date": datetime.datetime(2022, 9, 13, 0, 0),
    "end_date": None,
    "email": ["telemetry-alerts@mozilla.com", "skahmann@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
    "max_active_tis_per_dag": None,
}

tags = ["impact/tier_3", "repo/bigquery-etl"]

with DAG(
    "bqetl_sponsored_tiles_clients_daily",
    default_args=default_args,
    schedule_interval="0 4 * * *",
    doc_md=docs,
    tags=tags,
    catchup=False,
) as dag:

    wait_for_checks__fail_telemetry_derived__unified_metrics__v1 = ExternalTaskSensor(
        task_id="wait_for_checks__fail_telemetry_derived__unified_metrics__v1",
        external_dag_id="bqetl_unified",
        external_task_id="checks__fail_telemetry_derived__unified_metrics__v1",
        execution_delta=datetime.timedelta(seconds=3600),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_copy_deduplicate_all = ExternalTaskSensor(
        task_id="wait_for_copy_deduplicate_all",
        external_dag_id="copy_deduplicate",
        external_task_id="copy_deduplicate_all",
        execution_delta=datetime.timedelta(seconds=10800),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_copy_deduplicate_main_ping = ExternalTaskSensor(
        task_id="wait_for_copy_deduplicate_main_ping",
        external_dag_id="copy_deduplicate",
        external_task_id="copy_deduplicate_main_ping",
        execution_delta=datetime.timedelta(seconds=10800),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_telemetry_derived__clients_daily_joined__v1 = ExternalTaskSensor(
        task_id="wait_for_telemetry_derived__clients_daily_joined__v1",
        external_dag_id="bqetl_main_summary",
        external_task_id="telemetry_derived__clients_daily_joined__v1",
        execution_delta=datetime.timedelta(seconds=7200),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    sponsored_tiles_clients_daily_v1 = bigquery_etl_query(
        task_id="sponsored_tiles_clients_daily_v1",
        destination_table="sponsored_tiles_clients_daily_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="skahmann@mozilla.com",
        email=[
            "akommasani@mozilla.com",
            "cmorales@mozilla.com",
            "skahmann@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    sponsored_tiles_clients_daily_v1.set_upstream(
        wait_for_checks__fail_telemetry_derived__unified_metrics__v1
    )

    sponsored_tiles_clients_daily_v1.set_upstream(wait_for_copy_deduplicate_all)

    sponsored_tiles_clients_daily_v1.set_upstream(wait_for_copy_deduplicate_main_ping)

    sponsored_tiles_clients_daily_v1.set_upstream(
        wait_for_telemetry_derived__clients_daily_joined__v1
    )
