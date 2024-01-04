# Generated via https://github.com/mozilla/bigquery-etl/blob/main/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskMarker
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
import datetime
from utils.constants import ALLOWED_STATES, FAILED_STATES
from utils.gcp import bigquery_etl_query, gke_command, bigquery_dq_check

docs = """
### bqetl_amo_stats

Built from bigquery-etl repo, [`dags/bqetl_amo_stats.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_amo_stats.py)

#### Description

Add-on download and install statistics to power the
[addons.mozilla.org](https://addons.mozilla.org) (AMO) stats pages.

See the [post on the Add-Ons Blog](https://blog.mozilla.org/addons/2020/06/10/improvements-to-statistics-processing-on-amo/).

#### Owner

kik@mozilla.com

#### Tags

* impact/tier_1
* repo/bigquery-etl
"""


default_args = {
    "owner": "kik@mozilla.com",
    "start_date": datetime.datetime(2020, 6, 1, 0, 0),
    "end_date": None,
    "email": ["telemetry-alerts@mozilla.com", "kik@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
}

tags = ["impact/tier_1", "repo/bigquery-etl"]

with DAG(
    "bqetl_amo_stats",
    default_args=default_args,
    schedule_interval="0 3 * * *",
    doc_md=docs,
    tags=tags,
) as dag:
    amo_dev__amo_stats_dau__v2 = bigquery_etl_query(
        task_id="amo_dev__amo_stats_dau__v2",
        destination_table="amo_stats_dau_v2",
        dataset_id="amo_dev",
        project_id="moz-fx-data-shared-prod",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    amo_dev__amo_stats_installs__v3 = bigquery_etl_query(
        task_id="amo_dev__amo_stats_installs__v3",
        destination_table="amo_stats_installs_v3",
        dataset_id="amo_dev",
        project_id="moz-fx-data-shared-prod",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    amo_prod__amo_stats_dau__v2 = bigquery_etl_query(
        task_id="amo_prod__amo_stats_dau__v2",
        destination_table="amo_stats_dau_v2",
        dataset_id="amo_prod",
        project_id="moz-fx-data-shared-prod",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    amo_prod__amo_stats_installs__v3 = bigquery_etl_query(
        task_id="amo_prod__amo_stats_installs__v3",
        destination_table="amo_stats_installs_v3",
        dataset_id="amo_prod",
        project_id="moz-fx-data-shared-prod",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    amo_prod__desktop_addons_by_client__v1 = bigquery_etl_query(
        task_id="amo_prod__desktop_addons_by_client__v1",
        destination_table="desktop_addons_by_client_v1",
        dataset_id="amo_prod",
        project_id="moz-fx-data-shared-prod",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    amo_prod__fenix_addons_by_client__v1 = bigquery_etl_query(
        task_id="amo_prod__fenix_addons_by_client__v1",
        destination_table="fenix_addons_by_client_v1",
        dataset_id="amo_prod",
        project_id="moz-fx-data-shared-prod",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    amo_dev__amo_stats_dau__v2.set_upstream(amo_prod__amo_stats_dau__v2)

    amo_dev__amo_stats_installs__v3.set_upstream(amo_dev__amo_stats_dau__v2)

    amo_dev__amo_stats_installs__v3.set_upstream(amo_prod__amo_stats_installs__v3)

    amo_prod__amo_stats_dau__v2.set_upstream(amo_prod__desktop_addons_by_client__v1)

    amo_prod__amo_stats_dau__v2.set_upstream(amo_prod__fenix_addons_by_client__v1)

    wait_for_bq_main_events = ExternalTaskSensor(
        task_id="wait_for_bq_main_events",
        external_dag_id="copy_deduplicate",
        external_task_id="bq_main_events",
        execution_delta=datetime.timedelta(seconds=7200),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    amo_prod__amo_stats_installs__v3.set_upstream(wait_for_bq_main_events)
    wait_for_event_events = ExternalTaskSensor(
        task_id="wait_for_event_events",
        external_dag_id="copy_deduplicate",
        external_task_id="event_events",
        execution_delta=datetime.timedelta(seconds=7200),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    amo_prod__amo_stats_installs__v3.set_upstream(wait_for_event_events)

    wait_for_copy_deduplicate_main_ping = ExternalTaskSensor(
        task_id="wait_for_copy_deduplicate_main_ping",
        external_dag_id="copy_deduplicate",
        external_task_id="copy_deduplicate_main_ping",
        execution_delta=datetime.timedelta(seconds=7200),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    amo_prod__desktop_addons_by_client__v1.set_upstream(
        wait_for_copy_deduplicate_main_ping
    )

    wait_for_copy_deduplicate_all = ExternalTaskSensor(
        task_id="wait_for_copy_deduplicate_all",
        external_dag_id="copy_deduplicate",
        external_task_id="copy_deduplicate_all",
        execution_delta=datetime.timedelta(seconds=7200),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    amo_prod__fenix_addons_by_client__v1.set_upstream(wait_for_copy_deduplicate_all)
