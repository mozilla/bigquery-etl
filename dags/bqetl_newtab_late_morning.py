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
### bqetl_newtab_late_morning

Built from bigquery-etl repo, [`dags/bqetl_newtab_late_morning.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_newtab_late_morning.py)

#### Description

Schedules newtab related queries to run later to capture more recent activity.
#### Owner

mbowerman@mozilla.com

#### Tags

* impact/tier_1
* repo/bigquery-etl
"""


default_args = {
    "owner": "mbowerman@mozilla.com",
    "start_date": datetime.datetime(2025, 8, 20, 0, 0),
    "end_date": None,
    "email": ["telemetry-alerts@mozilla.com", "ctroy@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
    "max_active_tis_per_dag": None,
}

tags = ["impact/tier_1", "repo/bigquery-etl"]

with DAG(
    "bqetl_newtab_late_morning",
    default_args=default_args,
    schedule_interval="0 10 * * *",
    doc_md=docs,
    tags=tags,
    catchup=False,
) as dag:

    wait_for_telemetry_derived__newtab_visits__v1 = ExternalTaskSensor(
        task_id="wait_for_telemetry_derived__newtab_visits__v1",
        external_dag_id="bqetl_newtab",
        external_task_id="telemetry_derived__newtab_visits__v1",
        execution_delta=datetime.timedelta(seconds=36000),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    telemetry_derived__newtab_clients_daily__v1 = bigquery_etl_query(
        task_id="telemetry_derived__newtab_clients_daily__v1",
        destination_table="newtab_clients_daily_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="cbeck@mozilla.com",
        email=[
            "cbeck@mozilla.com",
            "ctroy@mozilla.com",
            "mbowerman@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    with TaskGroup(
        "telemetry_derived__newtab_clients_daily__v1_external",
    ) as telemetry_derived__newtab_clients_daily__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_ltv__wait_for_telemetry_derived__newtab_clients_daily__v1",
            external_dag_id="bqetl_ltv",
            external_task_id="wait_for_telemetry_derived__newtab_clients_daily__v1",
            execution_date="{{ (execution_date - macros.timedelta(seconds=36000)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_newtab__wait_for_telemetry_derived__newtab_clients_daily__v1",
            external_dag_id="bqetl_newtab",
            external_task_id="wait_for_telemetry_derived__newtab_clients_daily__v1",
            execution_date="{{ (execution_date - macros.timedelta(seconds=36000)).isoformat() }}",
        )

        telemetry_derived__newtab_clients_daily__v1_external.set_upstream(
            telemetry_derived__newtab_clients_daily__v1
        )

    telemetry_derived__newtab_clients_daily__v1.set_upstream(
        wait_for_telemetry_derived__newtab_visits__v1
    )
