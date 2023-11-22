# Generated via https://github.com/mozilla/bigquery-etl/blob/main/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskMarker
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
import datetime
from utils.constants import ALLOWED_STATES, FAILED_STATES
from utils.gcp import bigquery_etl_query, gke_command, bigquery_dq_check

docs = """
### bqetl_generated_funnels

Built from bigquery-etl repo, [`dags/bqetl_generated_funnels.py`](https://github.com/mozilla/bigquery-etl/blob/main/dags/bqetl_generated_funnels.py)

#### Description

DAG scheduling funnels defined in sql_generators/funnels
#### Owner

ascholtz@mozilla.com
"""


default_args = {
    "owner": "ascholtz@mozilla.com",
    "start_date": datetime.datetime(2023, 10, 14, 0, 0),
    "end_date": None,
    "email": ["telemetry-alerts@mozilla.com", "ascholtz@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
}

tags = ["impact/tier_3", "repo/bigquery-etl", "triage/no_triage"]

with DAG(
    "bqetl_generated_funnels",
    default_args=default_args,
    schedule_interval="0 5 * * *",
    doc_md=docs,
    tags=tags,
) as dag:
    firefox_accounts_derived__login_funnels__v1 = bigquery_etl_query(
        task_id="firefox_accounts_derived__login_funnels__v1",
        destination_table="login_funnels_v1",
        dataset_id="firefox_accounts_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ksiegler@mozilla.org",
        email=[
            "ascholtz@mozilla.com",
            "ksiegler@mozilla.org",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    wait_for_firefox_accounts_derived__fxa_auth_events__v1 = ExternalTaskSensor(
        task_id="wait_for_firefox_accounts_derived__fxa_auth_events__v1",
        external_dag_id="bqetl_fxa_events",
        external_task_id="firefox_accounts_derived__fxa_auth_events__v1",
        execution_delta=datetime.timedelta(seconds=12600),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    firefox_accounts_derived__login_funnels__v1.set_upstream(
        wait_for_firefox_accounts_derived__fxa_auth_events__v1
    )
    wait_for_firefox_accounts_derived__fxa_content_events__v1 = ExternalTaskSensor(
        task_id="wait_for_firefox_accounts_derived__fxa_content_events__v1",
        external_dag_id="bqetl_fxa_events",
        external_task_id="firefox_accounts_derived__fxa_content_events__v1",
        execution_delta=datetime.timedelta(seconds=12600),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    firefox_accounts_derived__login_funnels__v1.set_upstream(
        wait_for_firefox_accounts_derived__fxa_content_events__v1
    )
    wait_for_firefox_accounts_derived__fxa_gcp_stderr_events__v1 = ExternalTaskSensor(
        task_id="wait_for_firefox_accounts_derived__fxa_gcp_stderr_events__v1",
        external_dag_id="bqetl_fxa_events",
        external_task_id="firefox_accounts_derived__fxa_gcp_stderr_events__v1",
        execution_delta=datetime.timedelta(seconds=12600),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    firefox_accounts_derived__login_funnels__v1.set_upstream(
        wait_for_firefox_accounts_derived__fxa_gcp_stderr_events__v1
    )
    wait_for_firefox_accounts_derived__fxa_gcp_stdout_events__v1 = ExternalTaskSensor(
        task_id="wait_for_firefox_accounts_derived__fxa_gcp_stdout_events__v1",
        external_dag_id="bqetl_fxa_events",
        external_task_id="firefox_accounts_derived__fxa_gcp_stdout_events__v1",
        execution_delta=datetime.timedelta(seconds=12600),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    firefox_accounts_derived__login_funnels__v1.set_upstream(
        wait_for_firefox_accounts_derived__fxa_gcp_stdout_events__v1
    )
    wait_for_firefox_accounts_derived__fxa_stdout_events__v1 = ExternalTaskSensor(
        task_id="wait_for_firefox_accounts_derived__fxa_stdout_events__v1",
        external_dag_id="bqetl_fxa_events",
        external_task_id="firefox_accounts_derived__fxa_stdout_events__v1",
        execution_delta=datetime.timedelta(seconds=12600),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    firefox_accounts_derived__login_funnels__v1.set_upstream(
        wait_for_firefox_accounts_derived__fxa_stdout_events__v1
    )
