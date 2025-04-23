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
### bqetl_marketing_suppression_list

Built from bigquery-etl repo, [`dags/bqetl_marketing_suppression_list.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_marketing_suppression_list.py)

#### Description

Ingest marketing suppression lists into BigQuery

#### Owner

cbeck@mozilla.com

#### Tags

* impact/tier_2
* repo/bigquery-etl
"""


default_args = {
    "owner": "cbeck@mozilla.com",
    "start_date": datetime.datetime(2024, 4, 21, 0, 0),
    "end_date": None,
    "email": ["cbeck@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
    "max_active_tis_per_dag": None,
}

tags = ["impact/tier_2", "repo/bigquery-etl"]

with DAG(
    "bqetl_marketing_suppression_list",
    default_args=default_args,
    schedule_interval="0 3 * * *",
    doc_md=docs,
    tags=tags,
    catchup=False,
) as dag:

    wait_for_braze_external__braze_currents_firefox_hard_bounces__v1 = (
        ExternalTaskSensor(
            task_id="wait_for_braze_external__braze_currents_firefox_hard_bounces__v1",
            external_dag_id="bqetl_braze_currents",
            external_task_id="braze_external__braze_currents_firefox_hard_bounces__v1",
            execution_delta=datetime.timedelta(seconds=3600),
            check_existence=True,
            mode="reschedule",
            poke_interval=datetime.timedelta(minutes=5),
            allowed_states=ALLOWED_STATES,
            failed_states=FAILED_STATES,
            pool="DATA_ENG_EXTERNALTASKSENSOR",
        )
    )

    wait_for_braze_external__braze_currents_firefox_unsubscribe__v1 = (
        ExternalTaskSensor(
            task_id="wait_for_braze_external__braze_currents_firefox_unsubscribe__v1",
            external_dag_id="bqetl_braze_currents",
            external_task_id="braze_external__braze_currents_firefox_unsubscribe__v1",
            execution_delta=datetime.timedelta(seconds=3600),
            check_existence=True,
            mode="reschedule",
            poke_interval=datetime.timedelta(minutes=5),
            allowed_states=ALLOWED_STATES,
            failed_states=FAILED_STATES,
            pool="DATA_ENG_EXTERNALTASKSENSOR",
        )
    )

    wait_for_braze_external__braze_currents_mozilla_hard_bounces__v1 = (
        ExternalTaskSensor(
            task_id="wait_for_braze_external__braze_currents_mozilla_hard_bounces__v1",
            external_dag_id="bqetl_braze_currents",
            external_task_id="braze_external__braze_currents_mozilla_hard_bounces__v1",
            execution_delta=datetime.timedelta(seconds=3600),
            check_existence=True,
            mode="reschedule",
            poke_interval=datetime.timedelta(minutes=5),
            allowed_states=ALLOWED_STATES,
            failed_states=FAILED_STATES,
            pool="DATA_ENG_EXTERNALTASKSENSOR",
        )
    )

    checks__fail_marketing_suppression_list_derived__main_suppression_list__v1 = bigquery_dq_check(
        task_id="checks__fail_marketing_suppression_list_derived__main_suppression_list__v1",
        source_table="main_suppression_list_v1",
        dataset_id="marketing_suppression_list_derived",
        project_id="moz-fx-data-shared-prod",
        is_dq_check_fail=True,
        owner="cbeck@mozilla.com",
        email=["cbeck@mozilla.com"],
        depends_on_past=False,
        task_concurrency=1,
        retries=0,
    )

    checks__warn_marketing_suppression_list_derived__main_suppression_list__v1 = bigquery_dq_check(
        task_id="checks__warn_marketing_suppression_list_derived__main_suppression_list__v1",
        source_table="main_suppression_list_v1",
        dataset_id="marketing_suppression_list_derived",
        project_id="moz-fx-data-shared-prod",
        is_dq_check_fail=False,
        owner="cbeck@mozilla.com",
        email=["cbeck@mozilla.com"],
        depends_on_past=False,
        task_concurrency=1,
        retries=0,
    )

    marketing_suppression_list_derived__main_suppression_list__v1 = bigquery_etl_query(
        task_id="marketing_suppression_list_derived__main_suppression_list__v1",
        destination_table="main_suppression_list_v1",
        dataset_id="marketing_suppression_list_derived",
        project_id="moz-fx-data-shared-prod",
        owner="cbeck@mozilla.com",
        email=["cbeck@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    marketing_suppression_list_external__campaign_monitor_suppression_list__v1 = GKEPodOperator(
        task_id="marketing_suppression_list_external__campaign_monitor_suppression_list__v1",
        arguments=[
            "python",
            "sql/moz-fx-data-shared-prod/marketing_suppression_list_external/campaign_monitor_suppression_list_v1/query.py",
        ]
        + [
            "--api_key={{ var.value.campaign_monitor_api_key }}",
            "--client_id={{ var.value.campaign_monitor_client_id }}",
        ],
        image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
        owner="cbeck@mozilla.com",
        email=["cbeck@mozilla.com"],
    )

    marketing_suppression_list_external__send_suppression_list_update_to_campaign_monitor__v1 = GKEPodOperator(
        task_id="marketing_suppression_list_external__send_suppression_list_update_to_campaign_monitor__v1",
        arguments=[
            "python",
            "sql/moz-fx-data-shared-prod/marketing_suppression_list_external/send_suppression_list_update_to_campaign_monitor_v1/query.py",
        ]
        + [
            "--api_key={{ var.value.campaign_monitor_api_key }}",
            "--client_id={{ var.value.campaign_monitor_client_id }}",
        ],
        image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
        owner="cbeck@mozilla.com",
        email=["cbeck@mozilla.com"],
    )

    checks__fail_marketing_suppression_list_derived__main_suppression_list__v1.set_upstream(
        marketing_suppression_list_derived__main_suppression_list__v1
    )

    checks__warn_marketing_suppression_list_derived__main_suppression_list__v1.set_upstream(
        marketing_suppression_list_derived__main_suppression_list__v1
    )

    marketing_suppression_list_derived__main_suppression_list__v1.set_upstream(
        wait_for_braze_external__braze_currents_firefox_hard_bounces__v1
    )

    marketing_suppression_list_derived__main_suppression_list__v1.set_upstream(
        wait_for_braze_external__braze_currents_firefox_unsubscribe__v1
    )

    marketing_suppression_list_derived__main_suppression_list__v1.set_upstream(
        wait_for_braze_external__braze_currents_mozilla_hard_bounces__v1
    )

    marketing_suppression_list_derived__main_suppression_list__v1.set_upstream(
        marketing_suppression_list_external__campaign_monitor_suppression_list__v1
    )

    marketing_suppression_list_external__send_suppression_list_update_to_campaign_monitor__v1.set_upstream(
        marketing_suppression_list_derived__main_suppression_list__v1
    )

    marketing_suppression_list_external__send_suppression_list_update_to_campaign_monitor__v1.set_upstream(
        marketing_suppression_list_external__campaign_monitor_suppression_list__v1
    )
