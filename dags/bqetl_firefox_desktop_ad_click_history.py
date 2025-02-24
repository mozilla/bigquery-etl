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
### bqetl_firefox_desktop_ad_click_history

Built from bigquery-etl repo, [`dags/bqetl_firefox_desktop_ad_click_history.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_firefox_desktop_ad_click_history.py)

#### Description

Calculates # of historical ad clicks for Firefox Desktop clients

#### Owner

kwindau@mozilla.com

#### Tags

* impact/tier_2
* repo/bigquery-etl
"""


default_args = {
    "owner": "kwindau@mozilla.com",
    "start_date": datetime.datetime(2024, 7, 16, 0, 0),
    "end_date": None,
    "email": ["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
}

tags = ["impact/tier_2", "repo/bigquery-etl"]

with DAG(
    "bqetl_firefox_desktop_ad_click_history",
    default_args=default_args,
    schedule_interval="0 16 * * *",
    doc_md=docs,
    tags=tags,
    catchup=False,
) as dag:

    wait_for_search_derived__search_clients_daily__v8 = ExternalTaskSensor(
        task_id="wait_for_search_derived__search_clients_daily__v8",
        external_dag_id="bqetl_search",
        external_task_id="search_derived__search_clients_daily__v8",
        execution_delta=datetime.timedelta(seconds=46800),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_search_derived__search_clients_last_seen__v2 = ExternalTaskSensor(
        task_id="wait_for_search_derived__search_clients_last_seen__v2",
        external_dag_id="bqetl_search",
        external_task_id="search_derived__search_clients_last_seen__v2",
        execution_delta=datetime.timedelta(seconds=46800),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_telemetry_derived__clients_first_seen__v1 = ExternalTaskSensor(
        task_id="wait_for_telemetry_derived__clients_first_seen__v1",
        external_dag_id="bqetl_main_summary",
        external_task_id="telemetry_derived__clients_first_seen__v1",
        execution_delta=datetime.timedelta(seconds=50400),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    checks__fail_firefox_desktop_derived__adclick_history__v1 = bigquery_dq_check(
        task_id="checks__fail_firefox_desktop_derived__adclick_history__v1",
        source_table="adclick_history_v1",
        dataset_id="firefox_desktop_derived",
        project_id="moz-fx-data-shared-prod",
        is_dq_check_fail=True,
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        task_concurrency=1,
        parameters=["submission_date:DATE:{{ds}}"],
        retries=0,
    )

    firefox_desktop_derived__adclick_history__v1 = bigquery_etl_query(
        task_id="firefox_desktop_derived__adclick_history__v1",
        destination_table="adclick_history_v1",
        dataset_id="firefox_desktop_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=True,
        parameters=["submission_date:DATE:{{ds}}"],
    )

    firefox_desktop_derived__client_ltv__v1 = bigquery_etl_query(
        task_id="firefox_desktop_derived__client_ltv__v1",
        destination_table="client_ltv_v1",
        dataset_id="firefox_desktop_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=True,
        parameters=["submission_date:DATE:{{ds}}"],
    )

    firefox_desktop_derived__ltv_states__v1 = bigquery_etl_query(
        task_id="firefox_desktop_derived__ltv_states__v1",
        destination_table="ltv_states_v1",
        dataset_id="firefox_desktop_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    checks__fail_firefox_desktop_derived__adclick_history__v1.set_upstream(
        firefox_desktop_derived__adclick_history__v1
    )

    firefox_desktop_derived__adclick_history__v1.set_upstream(
        wait_for_search_derived__search_clients_daily__v8
    )

    firefox_desktop_derived__client_ltv__v1.set_upstream(
        firefox_desktop_derived__ltv_states__v1
    )

    firefox_desktop_derived__ltv_states__v1.set_upstream(
        checks__fail_firefox_desktop_derived__adclick_history__v1
    )

    firefox_desktop_derived__ltv_states__v1.set_upstream(
        wait_for_search_derived__search_clients_last_seen__v2
    )

    firefox_desktop_derived__ltv_states__v1.set_upstream(
        wait_for_telemetry_derived__clients_first_seen__v1
    )
