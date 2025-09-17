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
### bqetl_firefox_enterprise

Built from bigquery-etl repo, [`dags/bqetl_firefox_enterprise.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_firefox_enterprise.py)

#### Description

DAG for processing queries related to Firefox Enterprise.

#### Owner

kik@mozilla.com

#### Tags

* impact/tier_3
* repo/bigquery-etl
"""


default_args = {
    "owner": "kik@mozilla.com",
    "start_date": datetime.datetime(2025, 8, 21, 0, 0),
    "end_date": None,
    "email": ["kik@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "max_active_tis_per_dag": None,
}

tags = ["impact/tier_3", "repo/bigquery-etl"]

with DAG(
    "bqetl_firefox_enterprise",
    default_args=default_args,
    schedule_interval="0 6 * * *",
    doc_md=docs,
    tags=tags,
    catchup=False,
) as dag:

    wait_for_bigeye__firefox_desktop_derived__baseline_clients_first_seen__v1 = ExternalTaskSensor(
        task_id="wait_for_bigeye__firefox_desktop_derived__baseline_clients_first_seen__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="firefox_desktop.bigeye__firefox_desktop_derived__baseline_clients_first_seen__v1",
        execution_delta=datetime.timedelta(seconds=14400),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_bigeye__firefox_desktop_derived__baseline_clients_last_seen__v1 = ExternalTaskSensor(
        task_id="wait_for_bigeye__firefox_desktop_derived__baseline_clients_last_seen__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="firefox_desktop.bigeye__firefox_desktop_derived__baseline_clients_last_seen__v1",
        execution_delta=datetime.timedelta(seconds=14400),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_bigeye__firefox_desktop_derived__desktop_dau_distribution_id_history__v1 = ExternalTaskSensor(
        task_id="wait_for_bigeye__firefox_desktop_derived__desktop_dau_distribution_id_history__v1",
        external_dag_id="bqetl_analytics_tables",
        external_task_id="bigeye__firefox_desktop_derived__desktop_dau_distribution_id_history__v1",
        execution_delta=datetime.timedelta(seconds=14400),
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
        execution_delta=datetime.timedelta(seconds=18000),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    firefox_desktop_derived__enterprise_metrics__v1 = bigquery_etl_query(
        task_id="firefox_desktop_derived__enterprise_metrics__v1",
        destination_table="enterprise_metrics_v1",
        dataset_id="firefox_desktop_derived",
        project_id="moz-fx-data-shared-prod",
        owner="pissac@mozilla.com",
        email=["kik@mozilla.com", "pissac@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    firefox_desktop_derived__enterprise_metrics_clients__v1 = bigquery_etl_query(
        task_id="firefox_desktop_derived__enterprise_metrics_clients__v1",
        destination_table="enterprise_metrics_clients_v1",
        dataset_id="firefox_desktop_derived",
        project_id="moz-fx-data-shared-prod",
        owner="pissac@mozilla.com",
        email=["kik@mozilla.com", "pissac@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    firefox_desktop_derived__enterprise_metrics__v1.set_upstream(
        firefox_desktop_derived__enterprise_metrics_clients__v1
    )

    firefox_desktop_derived__enterprise_metrics_clients__v1.set_upstream(
        wait_for_bigeye__firefox_desktop_derived__baseline_clients_first_seen__v1
    )

    firefox_desktop_derived__enterprise_metrics_clients__v1.set_upstream(
        wait_for_bigeye__firefox_desktop_derived__baseline_clients_last_seen__v1
    )

    firefox_desktop_derived__enterprise_metrics_clients__v1.set_upstream(
        wait_for_bigeye__firefox_desktop_derived__desktop_dau_distribution_id_history__v1
    )

    firefox_desktop_derived__enterprise_metrics_clients__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )
