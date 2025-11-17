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
### bqetl_terms_of_use

Built from bigquery-etl repo, [`dags/bqetl_terms_of_use.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_terms_of_use.py)

#### Description

DAG for updating terms_of_use related datasets.
#### Owner

kik@mozilla.com

#### Tags

* impact/tier_2
* repo/bigquery-etl
"""


default_args = {
    "owner": "kik@mozilla.com",
    "start_date": datetime.datetime(2025, 11, 3, 0, 0),
    "end_date": None,
    "email": ["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "max_active_tis_per_dag": None,
}

tags = ["impact/tier_2", "repo/bigquery-etl"]

with DAG(
    "bqetl_terms_of_use",
    default_args=default_args,
    schedule_interval="0 6 * * *",
    doc_md=docs,
    tags=tags,
    catchup=False,
) as dag:

    task_group_fenix = TaskGroup("fenix")

    task_group_firefox_desktop = TaskGroup("firefox_desktop")

    task_group_firefox_ios = TaskGroup("firefox_ios")

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

    bigeye__fenix_derived__terms_of_use_status__v1 = bigquery_bigeye_check(
        task_id="bigeye__fenix_derived__terms_of_use_status__v1",
        table_id="moz-fx-data-shared-prod.fenix_derived.terms_of_use_status_v1",
        warehouse_id="1939",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
        task_group=task_group_fenix,
    )

    bigeye__firefox_desktop_derived__terms_of_use_events__v1 = bigquery_bigeye_check(
        task_id="bigeye__firefox_desktop_derived__terms_of_use_events__v1",
        table_id="moz-fx-data-shared-prod.firefox_desktop_derived.terms_of_use_events_v1",
        warehouse_id="1939",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
        task_group=task_group_firefox_desktop,
    )

    bigeye__firefox_desktop_derived__terms_of_use_status__v1 = bigquery_bigeye_check(
        task_id="bigeye__firefox_desktop_derived__terms_of_use_status__v1",
        table_id="moz-fx-data-shared-prod.firefox_desktop_derived.terms_of_use_status_v1",
        warehouse_id="1939",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
        task_group=task_group_firefox_desktop,
    )

    bigeye__firefox_ios_derived__terms_of_use_status__v1 = bigquery_bigeye_check(
        task_id="bigeye__firefox_ios_derived__terms_of_use_status__v1",
        table_id="moz-fx-data-shared-prod.firefox_ios_derived.terms_of_use_status_v1",
        warehouse_id="1939",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
        task_group=task_group_firefox_ios,
    )

    fenix_derived__terms_of_use_status__v1 = bigquery_etl_query(
        task_id="fenix_derived__terms_of_use_status__v1",
        destination_table="terms_of_use_status_v1",
        dataset_id="fenix_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=True,
        parameters=["submission_date:DATE:{{ds}}"],
        task_group=task_group_fenix,
    )

    firefox_desktop_derived__terms_of_use_events__v1 = bigquery_etl_query(
        task_id="firefox_desktop_derived__terms_of_use_events__v1",
        destination_table="terms_of_use_events_v1",
        dataset_id="firefox_desktop_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_firefox_desktop,
    )

    firefox_desktop_derived__terms_of_use_status__v1 = bigquery_etl_query(
        task_id="firefox_desktop_derived__terms_of_use_status__v1",
        destination_table="terms_of_use_status_v1",
        dataset_id="firefox_desktop_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=True,
        parameters=["submission_date:DATE:{{ds}}"],
        task_group=task_group_firefox_desktop,
    )

    firefox_ios_derived__terms_of_use_status__v1 = bigquery_etl_query(
        task_id="firefox_ios_derived__terms_of_use_status__v1",
        destination_table="terms_of_use_status_v1",
        dataset_id="firefox_ios_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=True,
        parameters=["submission_date:DATE:{{ds}}"],
        task_group=task_group_firefox_ios,
    )

    bigeye__fenix_derived__terms_of_use_status__v1.set_upstream(
        fenix_derived__terms_of_use_status__v1
    )

    bigeye__firefox_desktop_derived__terms_of_use_events__v1.set_upstream(
        firefox_desktop_derived__terms_of_use_events__v1
    )

    bigeye__firefox_desktop_derived__terms_of_use_status__v1.set_upstream(
        firefox_desktop_derived__terms_of_use_status__v1
    )

    bigeye__firefox_ios_derived__terms_of_use_status__v1.set_upstream(
        firefox_ios_derived__terms_of_use_status__v1
    )

    fenix_derived__terms_of_use_status__v1.set_upstream(wait_for_copy_deduplicate_all)

    firefox_desktop_derived__terms_of_use_events__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    firefox_desktop_derived__terms_of_use_status__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    firefox_ios_derived__terms_of_use_status__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )
