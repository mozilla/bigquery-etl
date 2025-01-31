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
### bqetl_ltv

Built from bigquery-etl repo, [`dags/bqetl_ltv.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_ltv.py)

#### Description

Schedules ltv related queries.
#### Owner

mbowerman@mozilla.com

#### Tags

* impact/tier_1
* repo/bigquery-etl
"""


default_args = {
    "owner": "mbowerman@mozilla.com",
    "start_date": datetime.datetime(2025, 1, 15, 0, 0),
    "end_date": None,
    "email": ["telemetry-alerts@mozilla.com", "mbowerman@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
}

tags = ["impact/tier_1", "repo/bigquery-etl"]

with DAG(
    "bqetl_ltv",
    default_args=default_args,
    schedule_interval="@daily",
    doc_md=docs,
    tags=tags,
) as dag:

    wait_for_copy_deduplicate_all = ExternalTaskSensor(
        task_id="wait_for_copy_deduplicate_all",
        external_dag_id="copy_deduplicate",
        external_task_id="copy_deduplicate_all",
        execution_delta=datetime.timedelta(days=-1, seconds=82800),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_search_derived__mobile_search_clients_daily__v2 = ExternalTaskSensor(
        task_id="wait_for_search_derived__mobile_search_clients_daily__v2",
        external_dag_id="bqetl_mobile_search",
        external_task_id="search_derived__mobile_search_clients_daily__v2",
        execution_delta=datetime.timedelta(days=-1, seconds=79200),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_search_derived__search_clients_daily__v8 = ExternalTaskSensor(
        task_id="wait_for_search_derived__search_clients_daily__v8",
        external_dag_id="bqetl_search",
        external_task_id="search_derived__search_clients_daily__v8",
        execution_delta=datetime.timedelta(days=-1, seconds=75600),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_telemetry_derived__newtab_clients_daily__v1 = ExternalTaskSensor(
        task_id="wait_for_telemetry_derived__newtab_clients_daily__v1",
        external_dag_id="bqetl_newtab",
        external_task_id="telemetry_derived__newtab_clients_daily__v1",
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    fenix_derived__ltv_android_aggregates__v1 = bigquery_etl_query(
        task_id="fenix_derived__ltv_android_aggregates__v1",
        destination_table="ltv_android_aggregates_v1",
        dataset_id="fenix_derived",
        project_id="moz-fx-data-shared-prod",
        owner="mbowerman@mozilla.com",
        email=["mbowerman@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    firefox_ios_derived__ltv_ios_aggregates__v1 = bigquery_etl_query(
        task_id="firefox_ios_derived__ltv_ios_aggregates__v1",
        destination_table="ltv_ios_aggregates_v1",
        dataset_id="firefox_ios_derived",
        project_id="moz-fx-data-shared-prod",
        owner="mbowerman@mozilla.com",
        email=["mbowerman@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    telemetry_derived__ltv_desktop_aggregates__v1 = bigquery_etl_query(
        task_id="telemetry_derived__ltv_desktop_aggregates__v1",
        destination_table="ltv_desktop_aggregates_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="mbowerman@mozilla.com",
        email=["mbowerman@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    fenix_derived__ltv_android_aggregates__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    fenix_derived__ltv_android_aggregates__v1.set_upstream(
        wait_for_search_derived__mobile_search_clients_daily__v2
    )

    firefox_ios_derived__ltv_ios_aggregates__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    firefox_ios_derived__ltv_ios_aggregates__v1.set_upstream(
        wait_for_search_derived__mobile_search_clients_daily__v2
    )

    telemetry_derived__ltv_desktop_aggregates__v1.set_upstream(
        wait_for_search_derived__search_clients_daily__v8
    )

    telemetry_derived__ltv_desktop_aggregates__v1.set_upstream(
        wait_for_telemetry_derived__newtab_clients_daily__v1
    )
