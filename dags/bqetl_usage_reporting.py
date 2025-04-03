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
### bqetl_usage_reporting

Built from bigquery-etl repo, [`dags/bqetl_usage_reporting.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_usage_reporting.py)

#### Description

DAG for updating usage_reporting related datasets.
#### Owner

kik@mozilla.com

#### Tags

* impact/tier_1
* repo/bigquery-etl
"""


default_args = {
    "owner": "kik@mozilla.com",
    "start_date": datetime.datetime(2025, 3, 19, 0, 0),
    "end_date": None,
    "email": ["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
}

tags = ["impact/tier_1", "repo/bigquery-etl"]

with DAG(
    "bqetl_usage_reporting",
    default_args=default_args,
    schedule_interval="0 4 * * *",
    doc_md=docs,
    tags=tags,
    catchup=False,
) as dag:

    task_group_fenix = TaskGroup("fenix")

    task_group_firefox_desktop = TaskGroup("firefox_desktop")

    task_group_firefox_ios = TaskGroup("firefox_ios")

    task_group_focus_android = TaskGroup("focus_android")

    task_group_focus_ios = TaskGroup("focus_ios")

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

    bigeye__fenix_derived__usage_reporting_active_users_aggregates__v1 = bigquery_bigeye_check(
        task_id="bigeye__fenix_derived__usage_reporting_active_users_aggregates__v1",
        table_id="moz-fx-data-shared-prod.fenix_derived.usage_reporting_active_users_aggregates_v1",
        warehouse_id="1939",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
        task_group=task_group_fenix,
    )

    bigeye__firefox_desktop_derived__usage_reporting_active_users_aggregates__v1 = bigquery_bigeye_check(
        task_id="bigeye__firefox_desktop_derived__usage_reporting_active_users_aggregates__v1",
        table_id="moz-fx-data-shared-prod.firefox_desktop_derived.usage_reporting_active_users_aggregates_v1",
        warehouse_id="1939",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
        task_group=task_group_firefox_desktop,
    )

    bigeye__firefox_desktop_derived__usage_reporting_clients_daily__v1 = bigquery_bigeye_check(
        task_id="bigeye__firefox_desktop_derived__usage_reporting_clients_daily__v1",
        table_id="moz-fx-data-shared-prod.firefox_desktop_derived.usage_reporting_clients_daily_v1",
        warehouse_id="1939",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
        task_group=task_group_firefox_desktop,
    )

    bigeye__firefox_desktop_derived__usage_reporting_clients_first_seen__v1 = bigquery_bigeye_check(
        task_id="bigeye__firefox_desktop_derived__usage_reporting_clients_first_seen__v1",
        table_id="moz-fx-data-shared-prod.firefox_desktop_derived.usage_reporting_clients_first_seen_v1",
        warehouse_id="1939",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
        task_group=task_group_firefox_desktop,
    )

    bigeye__firefox_desktop_derived__usage_reporting_clients_last_seen__v1 = bigquery_bigeye_check(
        task_id="bigeye__firefox_desktop_derived__usage_reporting_clients_last_seen__v1",
        table_id="moz-fx-data-shared-prod.firefox_desktop_derived.usage_reporting_clients_last_seen_v1",
        warehouse_id="1939",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
        task_group=task_group_firefox_desktop,
    )

    bigeye__firefox_ios_derived__usage_reporting_active_users_aggregates__v1 = bigquery_bigeye_check(
        task_id="bigeye__firefox_ios_derived__usage_reporting_active_users_aggregates__v1",
        table_id="moz-fx-data-shared-prod.firefox_ios_derived.usage_reporting_active_users_aggregates_v1",
        warehouse_id="1939",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
        task_group=task_group_firefox_ios,
    )

    bigeye__focus_android_derived__usage_reporting_active_users_aggregates__v1 = bigquery_bigeye_check(
        task_id="bigeye__focus_android_derived__usage_reporting_active_users_aggregates__v1",
        table_id="moz-fx-data-shared-prod.focus_android_derived.usage_reporting_active_users_aggregates_v1",
        warehouse_id="1939",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
        task_group=task_group_focus_android,
    )

    bigeye__focus_ios_derived__usage_reporting_active_users_aggregates__v1 = bigquery_bigeye_check(
        task_id="bigeye__focus_ios_derived__usage_reporting_active_users_aggregates__v1",
        table_id="moz-fx-data-shared-prod.focus_ios_derived.usage_reporting_active_users_aggregates_v1",
        warehouse_id="1939",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
        task_group=task_group_focus_ios,
    )

    bigeye__org_mozilla_fenix_derived__usage_reporting_clients_daily__v1 = bigquery_bigeye_check(
        task_id="bigeye__org_mozilla_fenix_derived__usage_reporting_clients_daily__v1",
        table_id="moz-fx-data-shared-prod.org_mozilla_fenix_derived.usage_reporting_clients_daily_v1",
        warehouse_id="1939",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
        task_group=task_group_fenix,
    )

    bigeye__org_mozilla_fenix_derived__usage_reporting_clients_first_seen__v1 = bigquery_bigeye_check(
        task_id="bigeye__org_mozilla_fenix_derived__usage_reporting_clients_first_seen__v1",
        table_id="moz-fx-data-shared-prod.org_mozilla_fenix_derived.usage_reporting_clients_first_seen_v1",
        warehouse_id="1939",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
        task_group=task_group_fenix,
    )

    bigeye__org_mozilla_fenix_derived__usage_reporting_clients_last_seen__v1 = bigquery_bigeye_check(
        task_id="bigeye__org_mozilla_fenix_derived__usage_reporting_clients_last_seen__v1",
        table_id="moz-fx-data-shared-prod.org_mozilla_fenix_derived.usage_reporting_clients_last_seen_v1",
        warehouse_id="1939",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
        task_group=task_group_fenix,
    )

    bigeye__org_mozilla_fenix_nightly_derived__usage_reporting_clients_daily__v1 = bigquery_bigeye_check(
        task_id="bigeye__org_mozilla_fenix_nightly_derived__usage_reporting_clients_daily__v1",
        table_id="moz-fx-data-shared-prod.org_mozilla_fenix_nightly_derived.usage_reporting_clients_daily_v1",
        warehouse_id="1939",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
        task_group=task_group_fenix,
    )

    bigeye__org_mozilla_fenix_nightly_derived__usage_reporting_clients_first_seen__v1 = bigquery_bigeye_check(
        task_id="bigeye__org_mozilla_fenix_nightly_derived__usage_reporting_clients_first_seen__v1",
        table_id="moz-fx-data-shared-prod.org_mozilla_fenix_nightly_derived.usage_reporting_clients_first_seen_v1",
        warehouse_id="1939",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
        task_group=task_group_fenix,
    )

    bigeye__org_mozilla_fenix_nightly_derived__usage_reporting_clients_last_seen__v1 = bigquery_bigeye_check(
        task_id="bigeye__org_mozilla_fenix_nightly_derived__usage_reporting_clients_last_seen__v1",
        table_id="moz-fx-data-shared-prod.org_mozilla_fenix_nightly_derived.usage_reporting_clients_last_seen_v1",
        warehouse_id="1939",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
        task_group=task_group_fenix,
    )

    bigeye__org_mozilla_fennec_aurora_derived__usage_reporting_clients_daily__v1 = bigquery_bigeye_check(
        task_id="bigeye__org_mozilla_fennec_aurora_derived__usage_reporting_clients_daily__v1",
        table_id="moz-fx-data-shared-prod.org_mozilla_fennec_aurora_derived.usage_reporting_clients_daily_v1",
        warehouse_id="1939",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
        task_group=task_group_fenix,
    )

    bigeye__org_mozilla_fennec_aurora_derived__usage_reporting_clients_first_seen__v1 = bigquery_bigeye_check(
        task_id="bigeye__org_mozilla_fennec_aurora_derived__usage_reporting_clients_first_seen__v1",
        table_id="moz-fx-data-shared-prod.org_mozilla_fennec_aurora_derived.usage_reporting_clients_first_seen_v1",
        warehouse_id="1939",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
        task_group=task_group_fenix,
    )

    bigeye__org_mozilla_fennec_aurora_derived__usage_reporting_clients_last_seen__v1 = bigquery_bigeye_check(
        task_id="bigeye__org_mozilla_fennec_aurora_derived__usage_reporting_clients_last_seen__v1",
        table_id="moz-fx-data-shared-prod.org_mozilla_fennec_aurora_derived.usage_reporting_clients_last_seen_v1",
        warehouse_id="1939",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
        task_group=task_group_fenix,
    )

    bigeye__org_mozilla_firefox_beta_derived__usage_reporting_clients_daily__v1 = bigquery_bigeye_check(
        task_id="bigeye__org_mozilla_firefox_beta_derived__usage_reporting_clients_daily__v1",
        table_id="moz-fx-data-shared-prod.org_mozilla_firefox_beta_derived.usage_reporting_clients_daily_v1",
        warehouse_id="1939",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
        task_group=task_group_fenix,
    )

    bigeye__org_mozilla_firefox_beta_derived__usage_reporting_clients_first_seen__v1 = bigquery_bigeye_check(
        task_id="bigeye__org_mozilla_firefox_beta_derived__usage_reporting_clients_first_seen__v1",
        table_id="moz-fx-data-shared-prod.org_mozilla_firefox_beta_derived.usage_reporting_clients_first_seen_v1",
        warehouse_id="1939",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
        task_group=task_group_fenix,
    )

    bigeye__org_mozilla_firefox_beta_derived__usage_reporting_clients_last_seen__v1 = bigquery_bigeye_check(
        task_id="bigeye__org_mozilla_firefox_beta_derived__usage_reporting_clients_last_seen__v1",
        table_id="moz-fx-data-shared-prod.org_mozilla_firefox_beta_derived.usage_reporting_clients_last_seen_v1",
        warehouse_id="1939",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
        task_group=task_group_fenix,
    )

    bigeye__org_mozilla_firefox_derived__usage_reporting_clients_daily__v1 = bigquery_bigeye_check(
        task_id="bigeye__org_mozilla_firefox_derived__usage_reporting_clients_daily__v1",
        table_id="moz-fx-data-shared-prod.org_mozilla_firefox_derived.usage_reporting_clients_daily_v1",
        warehouse_id="1939",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
        task_group=task_group_fenix,
    )

    bigeye__org_mozilla_firefox_derived__usage_reporting_clients_first_seen__v1 = bigquery_bigeye_check(
        task_id="bigeye__org_mozilla_firefox_derived__usage_reporting_clients_first_seen__v1",
        table_id="moz-fx-data-shared-prod.org_mozilla_firefox_derived.usage_reporting_clients_first_seen_v1",
        warehouse_id="1939",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
        task_group=task_group_fenix,
    )

    bigeye__org_mozilla_firefox_derived__usage_reporting_clients_last_seen__v1 = bigquery_bigeye_check(
        task_id="bigeye__org_mozilla_firefox_derived__usage_reporting_clients_last_seen__v1",
        table_id="moz-fx-data-shared-prod.org_mozilla_firefox_derived.usage_reporting_clients_last_seen_v1",
        warehouse_id="1939",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
        task_group=task_group_fenix,
    )

    bigeye__org_mozilla_focus_beta_derived__usage_reporting_clients_daily__v1 = bigquery_bigeye_check(
        task_id="bigeye__org_mozilla_focus_beta_derived__usage_reporting_clients_daily__v1",
        table_id="moz-fx-data-shared-prod.org_mozilla_focus_beta_derived.usage_reporting_clients_daily_v1",
        warehouse_id="1939",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
        task_group=task_group_focus_android,
    )

    bigeye__org_mozilla_focus_beta_derived__usage_reporting_clients_first_seen__v1 = bigquery_bigeye_check(
        task_id="bigeye__org_mozilla_focus_beta_derived__usage_reporting_clients_first_seen__v1",
        table_id="moz-fx-data-shared-prod.org_mozilla_focus_beta_derived.usage_reporting_clients_first_seen_v1",
        warehouse_id="1939",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
        task_group=task_group_focus_android,
    )

    bigeye__org_mozilla_focus_beta_derived__usage_reporting_clients_last_seen__v1 = bigquery_bigeye_check(
        task_id="bigeye__org_mozilla_focus_beta_derived__usage_reporting_clients_last_seen__v1",
        table_id="moz-fx-data-shared-prod.org_mozilla_focus_beta_derived.usage_reporting_clients_last_seen_v1",
        warehouse_id="1939",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
        task_group=task_group_focus_android,
    )

    bigeye__org_mozilla_focus_derived__usage_reporting_clients_daily__v1 = bigquery_bigeye_check(
        task_id="bigeye__org_mozilla_focus_derived__usage_reporting_clients_daily__v1",
        table_id="moz-fx-data-shared-prod.org_mozilla_focus_derived.usage_reporting_clients_daily_v1",
        warehouse_id="1939",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
        task_group=task_group_focus_android,
    )

    bigeye__org_mozilla_focus_derived__usage_reporting_clients_first_seen__v1 = bigquery_bigeye_check(
        task_id="bigeye__org_mozilla_focus_derived__usage_reporting_clients_first_seen__v1",
        table_id="moz-fx-data-shared-prod.org_mozilla_focus_derived.usage_reporting_clients_first_seen_v1",
        warehouse_id="1939",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
        task_group=task_group_focus_android,
    )

    bigeye__org_mozilla_focus_derived__usage_reporting_clients_last_seen__v1 = bigquery_bigeye_check(
        task_id="bigeye__org_mozilla_focus_derived__usage_reporting_clients_last_seen__v1",
        table_id="moz-fx-data-shared-prod.org_mozilla_focus_derived.usage_reporting_clients_last_seen_v1",
        warehouse_id="1939",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
        task_group=task_group_focus_android,
    )

    bigeye__org_mozilla_focus_nightly_derived__usage_reporting_clients_daily__v1 = bigquery_bigeye_check(
        task_id="bigeye__org_mozilla_focus_nightly_derived__usage_reporting_clients_daily__v1",
        table_id="moz-fx-data-shared-prod.org_mozilla_focus_nightly_derived.usage_reporting_clients_daily_v1",
        warehouse_id="1939",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
        task_group=task_group_focus_android,
    )

    bigeye__org_mozilla_focus_nightly_derived__usage_reporting_clients_first_seen__v1 = bigquery_bigeye_check(
        task_id="bigeye__org_mozilla_focus_nightly_derived__usage_reporting_clients_first_seen__v1",
        table_id="moz-fx-data-shared-prod.org_mozilla_focus_nightly_derived.usage_reporting_clients_first_seen_v1",
        warehouse_id="1939",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
        task_group=task_group_focus_android,
    )

    bigeye__org_mozilla_focus_nightly_derived__usage_reporting_clients_last_seen__v1 = bigquery_bigeye_check(
        task_id="bigeye__org_mozilla_focus_nightly_derived__usage_reporting_clients_last_seen__v1",
        table_id="moz-fx-data-shared-prod.org_mozilla_focus_nightly_derived.usage_reporting_clients_last_seen_v1",
        warehouse_id="1939",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
        task_group=task_group_focus_android,
    )

    bigeye__org_mozilla_ios_fennec_derived__usage_reporting_clients_daily__v1 = bigquery_bigeye_check(
        task_id="bigeye__org_mozilla_ios_fennec_derived__usage_reporting_clients_daily__v1",
        table_id="moz-fx-data-shared-prod.org_mozilla_ios_fennec_derived.usage_reporting_clients_daily_v1",
        warehouse_id="1939",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
        task_group=task_group_firefox_ios,
    )

    bigeye__org_mozilla_ios_fennec_derived__usage_reporting_clients_first_seen__v1 = bigquery_bigeye_check(
        task_id="bigeye__org_mozilla_ios_fennec_derived__usage_reporting_clients_first_seen__v1",
        table_id="moz-fx-data-shared-prod.org_mozilla_ios_fennec_derived.usage_reporting_clients_first_seen_v1",
        warehouse_id="1939",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
        task_group=task_group_firefox_ios,
    )

    bigeye__org_mozilla_ios_fennec_derived__usage_reporting_clients_last_seen__v1 = bigquery_bigeye_check(
        task_id="bigeye__org_mozilla_ios_fennec_derived__usage_reporting_clients_last_seen__v1",
        table_id="moz-fx-data-shared-prod.org_mozilla_ios_fennec_derived.usage_reporting_clients_last_seen_v1",
        warehouse_id="1939",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
        task_group=task_group_firefox_ios,
    )

    bigeye__org_mozilla_ios_firefox_derived__usage_reporting_clients_daily__v1 = bigquery_bigeye_check(
        task_id="bigeye__org_mozilla_ios_firefox_derived__usage_reporting_clients_daily__v1",
        table_id="moz-fx-data-shared-prod.org_mozilla_ios_firefox_derived.usage_reporting_clients_daily_v1",
        warehouse_id="1939",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
        task_group=task_group_firefox_ios,
    )

    bigeye__org_mozilla_ios_firefox_derived__usage_reporting_clients_first_seen__v1 = bigquery_bigeye_check(
        task_id="bigeye__org_mozilla_ios_firefox_derived__usage_reporting_clients_first_seen__v1",
        table_id="moz-fx-data-shared-prod.org_mozilla_ios_firefox_derived.usage_reporting_clients_first_seen_v1",
        warehouse_id="1939",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
        task_group=task_group_firefox_ios,
    )

    bigeye__org_mozilla_ios_firefox_derived__usage_reporting_clients_last_seen__v1 = bigquery_bigeye_check(
        task_id="bigeye__org_mozilla_ios_firefox_derived__usage_reporting_clients_last_seen__v1",
        table_id="moz-fx-data-shared-prod.org_mozilla_ios_firefox_derived.usage_reporting_clients_last_seen_v1",
        warehouse_id="1939",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
        task_group=task_group_firefox_ios,
    )

    bigeye__org_mozilla_ios_firefoxbeta_derived__usage_reporting_clients_daily__v1 = bigquery_bigeye_check(
        task_id="bigeye__org_mozilla_ios_firefoxbeta_derived__usage_reporting_clients_daily__v1",
        table_id="moz-fx-data-shared-prod.org_mozilla_ios_firefoxbeta_derived.usage_reporting_clients_daily_v1",
        warehouse_id="1939",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
        task_group=task_group_firefox_ios,
    )

    bigeye__org_mozilla_ios_firefoxbeta_derived__usage_reporting_clients_first_seen__v1 = bigquery_bigeye_check(
        task_id="bigeye__org_mozilla_ios_firefoxbeta_derived__usage_reporting_clients_first_seen__v1",
        table_id="moz-fx-data-shared-prod.org_mozilla_ios_firefoxbeta_derived.usage_reporting_clients_first_seen_v1",
        warehouse_id="1939",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
        task_group=task_group_firefox_ios,
    )

    bigeye__org_mozilla_ios_firefoxbeta_derived__usage_reporting_clients_last_seen__v1 = bigquery_bigeye_check(
        task_id="bigeye__org_mozilla_ios_firefoxbeta_derived__usage_reporting_clients_last_seen__v1",
        table_id="moz-fx-data-shared-prod.org_mozilla_ios_firefoxbeta_derived.usage_reporting_clients_last_seen_v1",
        warehouse_id="1939",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
        task_group=task_group_firefox_ios,
    )

    bigeye__org_mozilla_ios_focus_derived__usage_reporting_clients_daily__v1 = bigquery_bigeye_check(
        task_id="bigeye__org_mozilla_ios_focus_derived__usage_reporting_clients_daily__v1",
        table_id="moz-fx-data-shared-prod.org_mozilla_ios_focus_derived.usage_reporting_clients_daily_v1",
        warehouse_id="1939",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
        task_group=task_group_focus_ios,
    )

    bigeye__org_mozilla_ios_focus_derived__usage_reporting_clients_first_seen__v1 = bigquery_bigeye_check(
        task_id="bigeye__org_mozilla_ios_focus_derived__usage_reporting_clients_first_seen__v1",
        table_id="moz-fx-data-shared-prod.org_mozilla_ios_focus_derived.usage_reporting_clients_first_seen_v1",
        warehouse_id="1939",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
        task_group=task_group_focus_ios,
    )

    bigeye__org_mozilla_ios_focus_derived__usage_reporting_clients_last_seen__v1 = bigquery_bigeye_check(
        task_id="bigeye__org_mozilla_ios_focus_derived__usage_reporting_clients_last_seen__v1",
        table_id="moz-fx-data-shared-prod.org_mozilla_ios_focus_derived.usage_reporting_clients_last_seen_v1",
        warehouse_id="1939",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
        task_group=task_group_focus_ios,
    )

    fenix_derived__usage_reporting_active_users_aggregates__v1 = bigquery_etl_query(
        task_id="fenix_derived__usage_reporting_active_users_aggregates__v1",
        destination_table="usage_reporting_active_users_aggregates_v1",
        dataset_id="fenix_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_fenix,
    )

    firefox_desktop_derived__usage_reporting_active_users_aggregates__v1 = bigquery_etl_query(
        task_id="firefox_desktop_derived__usage_reporting_active_users_aggregates__v1",
        destination_table="usage_reporting_active_users_aggregates_v1",
        dataset_id="firefox_desktop_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_firefox_desktop,
    )

    firefox_desktop_derived__usage_reporting_clients_daily__v1 = bigquery_etl_query(
        task_id="firefox_desktop_derived__usage_reporting_clients_daily__v1",
        destination_table="usage_reporting_clients_daily_v1",
        dataset_id="firefox_desktop_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_firefox_desktop,
    )

    firefox_desktop_derived__usage_reporting_clients_first_seen__v1 = (
        bigquery_etl_query(
            task_id="firefox_desktop_derived__usage_reporting_clients_first_seen__v1",
            destination_table="usage_reporting_clients_first_seen_v1",
            dataset_id="firefox_desktop_derived",
            project_id="moz-fx-data-shared-prod",
            owner="kik@mozilla.com",
            email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
            date_partition_parameter="submission_date",
            depends_on_past=False,
            task_group=task_group_firefox_desktop,
        )
    )

    firefox_desktop_derived__usage_reporting_clients_last_seen__v1 = bigquery_etl_query(
        task_id="firefox_desktop_derived__usage_reporting_clients_last_seen__v1",
        destination_table="usage_reporting_clients_last_seen_v1",
        dataset_id="firefox_desktop_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_firefox_desktop,
    )

    firefox_ios_derived__usage_reporting_active_users_aggregates__v1 = (
        bigquery_etl_query(
            task_id="firefox_ios_derived__usage_reporting_active_users_aggregates__v1",
            destination_table="usage_reporting_active_users_aggregates_v1",
            dataset_id="firefox_ios_derived",
            project_id="moz-fx-data-shared-prod",
            owner="kik@mozilla.com",
            email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
            date_partition_parameter="submission_date",
            depends_on_past=False,
            task_group=task_group_firefox_ios,
        )
    )

    focus_android_derived__usage_reporting_active_users_aggregates__v1 = bigquery_etl_query(
        task_id="focus_android_derived__usage_reporting_active_users_aggregates__v1",
        destination_table="usage_reporting_active_users_aggregates_v1",
        dataset_id="focus_android_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_focus_android,
    )

    focus_ios_derived__usage_reporting_active_users_aggregates__v1 = bigquery_etl_query(
        task_id="focus_ios_derived__usage_reporting_active_users_aggregates__v1",
        destination_table="usage_reporting_active_users_aggregates_v1",
        dataset_id="focus_ios_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_focus_ios,
    )

    org_mozilla_fenix_derived__usage_reporting_clients_daily__v1 = bigquery_etl_query(
        task_id="org_mozilla_fenix_derived__usage_reporting_clients_daily__v1",
        destination_table="usage_reporting_clients_daily_v1",
        dataset_id="org_mozilla_fenix_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_fenix,
    )

    org_mozilla_fenix_derived__usage_reporting_clients_first_seen__v1 = (
        bigquery_etl_query(
            task_id="org_mozilla_fenix_derived__usage_reporting_clients_first_seen__v1",
            destination_table="usage_reporting_clients_first_seen_v1",
            dataset_id="org_mozilla_fenix_derived",
            project_id="moz-fx-data-shared-prod",
            owner="kik@mozilla.com",
            email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
            date_partition_parameter="submission_date",
            depends_on_past=False,
            task_group=task_group_fenix,
        )
    )

    org_mozilla_fenix_derived__usage_reporting_clients_last_seen__v1 = (
        bigquery_etl_query(
            task_id="org_mozilla_fenix_derived__usage_reporting_clients_last_seen__v1",
            destination_table="usage_reporting_clients_last_seen_v1",
            dataset_id="org_mozilla_fenix_derived",
            project_id="moz-fx-data-shared-prod",
            owner="kik@mozilla.com",
            email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
            date_partition_parameter="submission_date",
            depends_on_past=False,
            task_group=task_group_fenix,
        )
    )

    org_mozilla_fenix_nightly_derived__usage_reporting_clients_daily__v1 = bigquery_etl_query(
        task_id="org_mozilla_fenix_nightly_derived__usage_reporting_clients_daily__v1",
        destination_table="usage_reporting_clients_daily_v1",
        dataset_id="org_mozilla_fenix_nightly_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_fenix,
    )

    org_mozilla_fenix_nightly_derived__usage_reporting_clients_first_seen__v1 = bigquery_etl_query(
        task_id="org_mozilla_fenix_nightly_derived__usage_reporting_clients_first_seen__v1",
        destination_table="usage_reporting_clients_first_seen_v1",
        dataset_id="org_mozilla_fenix_nightly_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_fenix,
    )

    org_mozilla_fenix_nightly_derived__usage_reporting_clients_last_seen__v1 = bigquery_etl_query(
        task_id="org_mozilla_fenix_nightly_derived__usage_reporting_clients_last_seen__v1",
        destination_table="usage_reporting_clients_last_seen_v1",
        dataset_id="org_mozilla_fenix_nightly_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_fenix,
    )

    org_mozilla_fennec_aurora_derived__usage_reporting_clients_daily__v1 = bigquery_etl_query(
        task_id="org_mozilla_fennec_aurora_derived__usage_reporting_clients_daily__v1",
        destination_table="usage_reporting_clients_daily_v1",
        dataset_id="org_mozilla_fennec_aurora_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_fenix,
    )

    org_mozilla_fennec_aurora_derived__usage_reporting_clients_first_seen__v1 = bigquery_etl_query(
        task_id="org_mozilla_fennec_aurora_derived__usage_reporting_clients_first_seen__v1",
        destination_table="usage_reporting_clients_first_seen_v1",
        dataset_id="org_mozilla_fennec_aurora_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_fenix,
    )

    org_mozilla_fennec_aurora_derived__usage_reporting_clients_last_seen__v1 = bigquery_etl_query(
        task_id="org_mozilla_fennec_aurora_derived__usage_reporting_clients_last_seen__v1",
        destination_table="usage_reporting_clients_last_seen_v1",
        dataset_id="org_mozilla_fennec_aurora_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_fenix,
    )

    org_mozilla_firefox_beta_derived__usage_reporting_clients_daily__v1 = bigquery_etl_query(
        task_id="org_mozilla_firefox_beta_derived__usage_reporting_clients_daily__v1",
        destination_table="usage_reporting_clients_daily_v1",
        dataset_id="org_mozilla_firefox_beta_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_fenix,
    )

    org_mozilla_firefox_beta_derived__usage_reporting_clients_first_seen__v1 = bigquery_etl_query(
        task_id="org_mozilla_firefox_beta_derived__usage_reporting_clients_first_seen__v1",
        destination_table="usage_reporting_clients_first_seen_v1",
        dataset_id="org_mozilla_firefox_beta_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_fenix,
    )

    org_mozilla_firefox_beta_derived__usage_reporting_clients_last_seen__v1 = bigquery_etl_query(
        task_id="org_mozilla_firefox_beta_derived__usage_reporting_clients_last_seen__v1",
        destination_table="usage_reporting_clients_last_seen_v1",
        dataset_id="org_mozilla_firefox_beta_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_fenix,
    )

    org_mozilla_firefox_derived__usage_reporting_clients_daily__v1 = bigquery_etl_query(
        task_id="org_mozilla_firefox_derived__usage_reporting_clients_daily__v1",
        destination_table="usage_reporting_clients_daily_v1",
        dataset_id="org_mozilla_firefox_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_fenix,
    )

    org_mozilla_firefox_derived__usage_reporting_clients_first_seen__v1 = bigquery_etl_query(
        task_id="org_mozilla_firefox_derived__usage_reporting_clients_first_seen__v1",
        destination_table="usage_reporting_clients_first_seen_v1",
        dataset_id="org_mozilla_firefox_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_fenix,
    )

    org_mozilla_firefox_derived__usage_reporting_clients_last_seen__v1 = bigquery_etl_query(
        task_id="org_mozilla_firefox_derived__usage_reporting_clients_last_seen__v1",
        destination_table="usage_reporting_clients_last_seen_v1",
        dataset_id="org_mozilla_firefox_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_fenix,
    )

    org_mozilla_focus_beta_derived__usage_reporting_clients_daily__v1 = (
        bigquery_etl_query(
            task_id="org_mozilla_focus_beta_derived__usage_reporting_clients_daily__v1",
            destination_table="usage_reporting_clients_daily_v1",
            dataset_id="org_mozilla_focus_beta_derived",
            project_id="moz-fx-data-shared-prod",
            owner="kik@mozilla.com",
            email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
            date_partition_parameter="submission_date",
            depends_on_past=False,
            task_group=task_group_focus_android,
        )
    )

    org_mozilla_focus_beta_derived__usage_reporting_clients_first_seen__v1 = bigquery_etl_query(
        task_id="org_mozilla_focus_beta_derived__usage_reporting_clients_first_seen__v1",
        destination_table="usage_reporting_clients_first_seen_v1",
        dataset_id="org_mozilla_focus_beta_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_focus_android,
    )

    org_mozilla_focus_beta_derived__usage_reporting_clients_last_seen__v1 = bigquery_etl_query(
        task_id="org_mozilla_focus_beta_derived__usage_reporting_clients_last_seen__v1",
        destination_table="usage_reporting_clients_last_seen_v1",
        dataset_id="org_mozilla_focus_beta_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_focus_android,
    )

    org_mozilla_focus_derived__usage_reporting_clients_daily__v1 = bigquery_etl_query(
        task_id="org_mozilla_focus_derived__usage_reporting_clients_daily__v1",
        destination_table="usage_reporting_clients_daily_v1",
        dataset_id="org_mozilla_focus_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_focus_android,
    )

    org_mozilla_focus_derived__usage_reporting_clients_first_seen__v1 = (
        bigquery_etl_query(
            task_id="org_mozilla_focus_derived__usage_reporting_clients_first_seen__v1",
            destination_table="usage_reporting_clients_first_seen_v1",
            dataset_id="org_mozilla_focus_derived",
            project_id="moz-fx-data-shared-prod",
            owner="kik@mozilla.com",
            email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
            date_partition_parameter="submission_date",
            depends_on_past=False,
            task_group=task_group_focus_android,
        )
    )

    org_mozilla_focus_derived__usage_reporting_clients_last_seen__v1 = (
        bigquery_etl_query(
            task_id="org_mozilla_focus_derived__usage_reporting_clients_last_seen__v1",
            destination_table="usage_reporting_clients_last_seen_v1",
            dataset_id="org_mozilla_focus_derived",
            project_id="moz-fx-data-shared-prod",
            owner="kik@mozilla.com",
            email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
            date_partition_parameter="submission_date",
            depends_on_past=False,
            task_group=task_group_focus_android,
        )
    )

    org_mozilla_focus_nightly_derived__usage_reporting_clients_daily__v1 = bigquery_etl_query(
        task_id="org_mozilla_focus_nightly_derived__usage_reporting_clients_daily__v1",
        destination_table="usage_reporting_clients_daily_v1",
        dataset_id="org_mozilla_focus_nightly_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_focus_android,
    )

    org_mozilla_focus_nightly_derived__usage_reporting_clients_first_seen__v1 = bigquery_etl_query(
        task_id="org_mozilla_focus_nightly_derived__usage_reporting_clients_first_seen__v1",
        destination_table="usage_reporting_clients_first_seen_v1",
        dataset_id="org_mozilla_focus_nightly_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_focus_android,
    )

    org_mozilla_focus_nightly_derived__usage_reporting_clients_last_seen__v1 = bigquery_etl_query(
        task_id="org_mozilla_focus_nightly_derived__usage_reporting_clients_last_seen__v1",
        destination_table="usage_reporting_clients_last_seen_v1",
        dataset_id="org_mozilla_focus_nightly_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_focus_android,
    )

    org_mozilla_ios_fennec_derived__usage_reporting_clients_daily__v1 = (
        bigquery_etl_query(
            task_id="org_mozilla_ios_fennec_derived__usage_reporting_clients_daily__v1",
            destination_table="usage_reporting_clients_daily_v1",
            dataset_id="org_mozilla_ios_fennec_derived",
            project_id="moz-fx-data-shared-prod",
            owner="kik@mozilla.com",
            email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
            date_partition_parameter="submission_date",
            depends_on_past=False,
            task_group=task_group_firefox_ios,
        )
    )

    org_mozilla_ios_fennec_derived__usage_reporting_clients_first_seen__v1 = bigquery_etl_query(
        task_id="org_mozilla_ios_fennec_derived__usage_reporting_clients_first_seen__v1",
        destination_table="usage_reporting_clients_first_seen_v1",
        dataset_id="org_mozilla_ios_fennec_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_firefox_ios,
    )

    org_mozilla_ios_fennec_derived__usage_reporting_clients_last_seen__v1 = bigquery_etl_query(
        task_id="org_mozilla_ios_fennec_derived__usage_reporting_clients_last_seen__v1",
        destination_table="usage_reporting_clients_last_seen_v1",
        dataset_id="org_mozilla_ios_fennec_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_firefox_ios,
    )

    org_mozilla_ios_firefox_derived__usage_reporting_clients_daily__v1 = bigquery_etl_query(
        task_id="org_mozilla_ios_firefox_derived__usage_reporting_clients_daily__v1",
        destination_table="usage_reporting_clients_daily_v1",
        dataset_id="org_mozilla_ios_firefox_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_firefox_ios,
    )

    org_mozilla_ios_firefox_derived__usage_reporting_clients_first_seen__v1 = bigquery_etl_query(
        task_id="org_mozilla_ios_firefox_derived__usage_reporting_clients_first_seen__v1",
        destination_table="usage_reporting_clients_first_seen_v1",
        dataset_id="org_mozilla_ios_firefox_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_firefox_ios,
    )

    org_mozilla_ios_firefox_derived__usage_reporting_clients_last_seen__v1 = bigquery_etl_query(
        task_id="org_mozilla_ios_firefox_derived__usage_reporting_clients_last_seen__v1",
        destination_table="usage_reporting_clients_last_seen_v1",
        dataset_id="org_mozilla_ios_firefox_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_firefox_ios,
    )

    org_mozilla_ios_firefoxbeta_derived__usage_reporting_clients_daily__v1 = bigquery_etl_query(
        task_id="org_mozilla_ios_firefoxbeta_derived__usage_reporting_clients_daily__v1",
        destination_table="usage_reporting_clients_daily_v1",
        dataset_id="org_mozilla_ios_firefoxbeta_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_firefox_ios,
    )

    org_mozilla_ios_firefoxbeta_derived__usage_reporting_clients_first_seen__v1 = bigquery_etl_query(
        task_id="org_mozilla_ios_firefoxbeta_derived__usage_reporting_clients_first_seen__v1",
        destination_table="usage_reporting_clients_first_seen_v1",
        dataset_id="org_mozilla_ios_firefoxbeta_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_firefox_ios,
    )

    org_mozilla_ios_firefoxbeta_derived__usage_reporting_clients_last_seen__v1 = bigquery_etl_query(
        task_id="org_mozilla_ios_firefoxbeta_derived__usage_reporting_clients_last_seen__v1",
        destination_table="usage_reporting_clients_last_seen_v1",
        dataset_id="org_mozilla_ios_firefoxbeta_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_firefox_ios,
    )

    org_mozilla_ios_focus_derived__usage_reporting_clients_daily__v1 = (
        bigquery_etl_query(
            task_id="org_mozilla_ios_focus_derived__usage_reporting_clients_daily__v1",
            destination_table="usage_reporting_clients_daily_v1",
            dataset_id="org_mozilla_ios_focus_derived",
            project_id="moz-fx-data-shared-prod",
            owner="kik@mozilla.com",
            email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
            date_partition_parameter="submission_date",
            depends_on_past=False,
            task_group=task_group_focus_ios,
        )
    )

    org_mozilla_ios_focus_derived__usage_reporting_clients_first_seen__v1 = bigquery_etl_query(
        task_id="org_mozilla_ios_focus_derived__usage_reporting_clients_first_seen__v1",
        destination_table="usage_reporting_clients_first_seen_v1",
        dataset_id="org_mozilla_ios_focus_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_focus_ios,
    )

    org_mozilla_ios_focus_derived__usage_reporting_clients_last_seen__v1 = bigquery_etl_query(
        task_id="org_mozilla_ios_focus_derived__usage_reporting_clients_last_seen__v1",
        destination_table="usage_reporting_clients_last_seen_v1",
        dataset_id="org_mozilla_ios_focus_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_focus_ios,
    )

    bigeye__fenix_derived__usage_reporting_active_users_aggregates__v1.set_upstream(
        fenix_derived__usage_reporting_active_users_aggregates__v1
    )

    bigeye__firefox_desktop_derived__usage_reporting_active_users_aggregates__v1.set_upstream(
        firefox_desktop_derived__usage_reporting_active_users_aggregates__v1
    )

    bigeye__firefox_desktop_derived__usage_reporting_clients_daily__v1.set_upstream(
        firefox_desktop_derived__usage_reporting_clients_daily__v1
    )

    bigeye__firefox_desktop_derived__usage_reporting_clients_first_seen__v1.set_upstream(
        firefox_desktop_derived__usage_reporting_clients_first_seen__v1
    )

    bigeye__firefox_desktop_derived__usage_reporting_clients_last_seen__v1.set_upstream(
        firefox_desktop_derived__usage_reporting_clients_last_seen__v1
    )

    bigeye__firefox_ios_derived__usage_reporting_active_users_aggregates__v1.set_upstream(
        firefox_ios_derived__usage_reporting_active_users_aggregates__v1
    )

    bigeye__focus_android_derived__usage_reporting_active_users_aggregates__v1.set_upstream(
        focus_android_derived__usage_reporting_active_users_aggregates__v1
    )

    bigeye__focus_ios_derived__usage_reporting_active_users_aggregates__v1.set_upstream(
        focus_ios_derived__usage_reporting_active_users_aggregates__v1
    )

    bigeye__org_mozilla_fenix_derived__usage_reporting_clients_daily__v1.set_upstream(
        org_mozilla_fenix_derived__usage_reporting_clients_daily__v1
    )

    bigeye__org_mozilla_fenix_derived__usage_reporting_clients_first_seen__v1.set_upstream(
        org_mozilla_fenix_derived__usage_reporting_clients_first_seen__v1
    )

    bigeye__org_mozilla_fenix_derived__usage_reporting_clients_last_seen__v1.set_upstream(
        org_mozilla_fenix_derived__usage_reporting_clients_last_seen__v1
    )

    bigeye__org_mozilla_fenix_nightly_derived__usage_reporting_clients_daily__v1.set_upstream(
        org_mozilla_fenix_nightly_derived__usage_reporting_clients_daily__v1
    )

    bigeye__org_mozilla_fenix_nightly_derived__usage_reporting_clients_first_seen__v1.set_upstream(
        org_mozilla_fenix_nightly_derived__usage_reporting_clients_first_seen__v1
    )

    bigeye__org_mozilla_fenix_nightly_derived__usage_reporting_clients_last_seen__v1.set_upstream(
        org_mozilla_fenix_nightly_derived__usage_reporting_clients_last_seen__v1
    )

    bigeye__org_mozilla_fennec_aurora_derived__usage_reporting_clients_daily__v1.set_upstream(
        org_mozilla_fennec_aurora_derived__usage_reporting_clients_daily__v1
    )

    bigeye__org_mozilla_fennec_aurora_derived__usage_reporting_clients_first_seen__v1.set_upstream(
        org_mozilla_fennec_aurora_derived__usage_reporting_clients_first_seen__v1
    )

    bigeye__org_mozilla_fennec_aurora_derived__usage_reporting_clients_last_seen__v1.set_upstream(
        org_mozilla_fennec_aurora_derived__usage_reporting_clients_last_seen__v1
    )

    bigeye__org_mozilla_firefox_beta_derived__usage_reporting_clients_daily__v1.set_upstream(
        org_mozilla_firefox_beta_derived__usage_reporting_clients_daily__v1
    )

    bigeye__org_mozilla_firefox_beta_derived__usage_reporting_clients_first_seen__v1.set_upstream(
        org_mozilla_firefox_beta_derived__usage_reporting_clients_first_seen__v1
    )

    bigeye__org_mozilla_firefox_beta_derived__usage_reporting_clients_last_seen__v1.set_upstream(
        org_mozilla_firefox_beta_derived__usage_reporting_clients_last_seen__v1
    )

    bigeye__org_mozilla_firefox_derived__usage_reporting_clients_daily__v1.set_upstream(
        org_mozilla_firefox_derived__usage_reporting_clients_daily__v1
    )

    bigeye__org_mozilla_firefox_derived__usage_reporting_clients_first_seen__v1.set_upstream(
        org_mozilla_firefox_derived__usage_reporting_clients_first_seen__v1
    )

    bigeye__org_mozilla_firefox_derived__usage_reporting_clients_last_seen__v1.set_upstream(
        org_mozilla_firefox_derived__usage_reporting_clients_last_seen__v1
    )

    bigeye__org_mozilla_focus_beta_derived__usage_reporting_clients_daily__v1.set_upstream(
        org_mozilla_focus_beta_derived__usage_reporting_clients_daily__v1
    )

    bigeye__org_mozilla_focus_beta_derived__usage_reporting_clients_first_seen__v1.set_upstream(
        org_mozilla_focus_beta_derived__usage_reporting_clients_first_seen__v1
    )

    bigeye__org_mozilla_focus_beta_derived__usage_reporting_clients_last_seen__v1.set_upstream(
        org_mozilla_focus_beta_derived__usage_reporting_clients_last_seen__v1
    )

    bigeye__org_mozilla_focus_derived__usage_reporting_clients_daily__v1.set_upstream(
        org_mozilla_focus_derived__usage_reporting_clients_daily__v1
    )

    bigeye__org_mozilla_focus_derived__usage_reporting_clients_first_seen__v1.set_upstream(
        org_mozilla_focus_derived__usage_reporting_clients_first_seen__v1
    )

    bigeye__org_mozilla_focus_derived__usage_reporting_clients_last_seen__v1.set_upstream(
        org_mozilla_focus_derived__usage_reporting_clients_last_seen__v1
    )

    bigeye__org_mozilla_focus_nightly_derived__usage_reporting_clients_daily__v1.set_upstream(
        org_mozilla_focus_nightly_derived__usage_reporting_clients_daily__v1
    )

    bigeye__org_mozilla_focus_nightly_derived__usage_reporting_clients_first_seen__v1.set_upstream(
        org_mozilla_focus_nightly_derived__usage_reporting_clients_first_seen__v1
    )

    bigeye__org_mozilla_focus_nightly_derived__usage_reporting_clients_last_seen__v1.set_upstream(
        org_mozilla_focus_nightly_derived__usage_reporting_clients_last_seen__v1
    )

    bigeye__org_mozilla_ios_fennec_derived__usage_reporting_clients_daily__v1.set_upstream(
        org_mozilla_ios_fennec_derived__usage_reporting_clients_daily__v1
    )

    bigeye__org_mozilla_ios_fennec_derived__usage_reporting_clients_first_seen__v1.set_upstream(
        org_mozilla_ios_fennec_derived__usage_reporting_clients_first_seen__v1
    )

    bigeye__org_mozilla_ios_fennec_derived__usage_reporting_clients_last_seen__v1.set_upstream(
        org_mozilla_ios_fennec_derived__usage_reporting_clients_last_seen__v1
    )

    bigeye__org_mozilla_ios_firefox_derived__usage_reporting_clients_daily__v1.set_upstream(
        org_mozilla_ios_firefox_derived__usage_reporting_clients_daily__v1
    )

    bigeye__org_mozilla_ios_firefox_derived__usage_reporting_clients_first_seen__v1.set_upstream(
        org_mozilla_ios_firefox_derived__usage_reporting_clients_first_seen__v1
    )

    bigeye__org_mozilla_ios_firefox_derived__usage_reporting_clients_last_seen__v1.set_upstream(
        org_mozilla_ios_firefox_derived__usage_reporting_clients_last_seen__v1
    )

    bigeye__org_mozilla_ios_firefoxbeta_derived__usage_reporting_clients_daily__v1.set_upstream(
        org_mozilla_ios_firefoxbeta_derived__usage_reporting_clients_daily__v1
    )

    bigeye__org_mozilla_ios_firefoxbeta_derived__usage_reporting_clients_first_seen__v1.set_upstream(
        org_mozilla_ios_firefoxbeta_derived__usage_reporting_clients_first_seen__v1
    )

    bigeye__org_mozilla_ios_firefoxbeta_derived__usage_reporting_clients_last_seen__v1.set_upstream(
        org_mozilla_ios_firefoxbeta_derived__usage_reporting_clients_last_seen__v1
    )

    bigeye__org_mozilla_ios_focus_derived__usage_reporting_clients_daily__v1.set_upstream(
        org_mozilla_ios_focus_derived__usage_reporting_clients_daily__v1
    )

    bigeye__org_mozilla_ios_focus_derived__usage_reporting_clients_first_seen__v1.set_upstream(
        org_mozilla_ios_focus_derived__usage_reporting_clients_first_seen__v1
    )

    bigeye__org_mozilla_ios_focus_derived__usage_reporting_clients_last_seen__v1.set_upstream(
        org_mozilla_ios_focus_derived__usage_reporting_clients_last_seen__v1
    )

    fenix_derived__usage_reporting_active_users_aggregates__v1.set_upstream(
        bigeye__org_mozilla_fenix_derived__usage_reporting_clients_daily__v1
    )

    fenix_derived__usage_reporting_active_users_aggregates__v1.set_upstream(
        bigeye__org_mozilla_fenix_derived__usage_reporting_clients_first_seen__v1
    )

    fenix_derived__usage_reporting_active_users_aggregates__v1.set_upstream(
        bigeye__org_mozilla_fenix_derived__usage_reporting_clients_last_seen__v1
    )

    fenix_derived__usage_reporting_active_users_aggregates__v1.set_upstream(
        bigeye__org_mozilla_fenix_nightly_derived__usage_reporting_clients_daily__v1
    )

    fenix_derived__usage_reporting_active_users_aggregates__v1.set_upstream(
        bigeye__org_mozilla_fenix_nightly_derived__usage_reporting_clients_first_seen__v1
    )

    fenix_derived__usage_reporting_active_users_aggregates__v1.set_upstream(
        bigeye__org_mozilla_fenix_nightly_derived__usage_reporting_clients_last_seen__v1
    )

    fenix_derived__usage_reporting_active_users_aggregates__v1.set_upstream(
        bigeye__org_mozilla_fennec_aurora_derived__usage_reporting_clients_daily__v1
    )

    fenix_derived__usage_reporting_active_users_aggregates__v1.set_upstream(
        bigeye__org_mozilla_fennec_aurora_derived__usage_reporting_clients_first_seen__v1
    )

    fenix_derived__usage_reporting_active_users_aggregates__v1.set_upstream(
        bigeye__org_mozilla_fennec_aurora_derived__usage_reporting_clients_last_seen__v1
    )

    fenix_derived__usage_reporting_active_users_aggregates__v1.set_upstream(
        bigeye__org_mozilla_firefox_beta_derived__usage_reporting_clients_daily__v1
    )

    fenix_derived__usage_reporting_active_users_aggregates__v1.set_upstream(
        bigeye__org_mozilla_firefox_beta_derived__usage_reporting_clients_first_seen__v1
    )

    fenix_derived__usage_reporting_active_users_aggregates__v1.set_upstream(
        bigeye__org_mozilla_firefox_beta_derived__usage_reporting_clients_last_seen__v1
    )

    fenix_derived__usage_reporting_active_users_aggregates__v1.set_upstream(
        bigeye__org_mozilla_firefox_derived__usage_reporting_clients_daily__v1
    )

    fenix_derived__usage_reporting_active_users_aggregates__v1.set_upstream(
        bigeye__org_mozilla_firefox_derived__usage_reporting_clients_first_seen__v1
    )

    fenix_derived__usage_reporting_active_users_aggregates__v1.set_upstream(
        bigeye__org_mozilla_firefox_derived__usage_reporting_clients_last_seen__v1
    )

    firefox_desktop_derived__usage_reporting_active_users_aggregates__v1.set_upstream(
        bigeye__firefox_desktop_derived__usage_reporting_clients_daily__v1
    )

    firefox_desktop_derived__usage_reporting_active_users_aggregates__v1.set_upstream(
        bigeye__firefox_desktop_derived__usage_reporting_clients_first_seen__v1
    )

    firefox_desktop_derived__usage_reporting_active_users_aggregates__v1.set_upstream(
        bigeye__firefox_desktop_derived__usage_reporting_clients_last_seen__v1
    )

    firefox_desktop_derived__usage_reporting_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    firefox_desktop_derived__usage_reporting_clients_first_seen__v1.set_upstream(
        bigeye__firefox_desktop_derived__usage_reporting_clients_daily__v1
    )

    firefox_desktop_derived__usage_reporting_clients_last_seen__v1.set_upstream(
        bigeye__firefox_desktop_derived__usage_reporting_clients_daily__v1
    )

    firefox_ios_derived__usage_reporting_active_users_aggregates__v1.set_upstream(
        bigeye__org_mozilla_ios_fennec_derived__usage_reporting_clients_daily__v1
    )

    firefox_ios_derived__usage_reporting_active_users_aggregates__v1.set_upstream(
        bigeye__org_mozilla_ios_fennec_derived__usage_reporting_clients_first_seen__v1
    )

    firefox_ios_derived__usage_reporting_active_users_aggregates__v1.set_upstream(
        bigeye__org_mozilla_ios_fennec_derived__usage_reporting_clients_last_seen__v1
    )

    firefox_ios_derived__usage_reporting_active_users_aggregates__v1.set_upstream(
        bigeye__org_mozilla_ios_firefox_derived__usage_reporting_clients_daily__v1
    )

    firefox_ios_derived__usage_reporting_active_users_aggregates__v1.set_upstream(
        bigeye__org_mozilla_ios_firefox_derived__usage_reporting_clients_first_seen__v1
    )

    firefox_ios_derived__usage_reporting_active_users_aggregates__v1.set_upstream(
        bigeye__org_mozilla_ios_firefox_derived__usage_reporting_clients_last_seen__v1
    )

    firefox_ios_derived__usage_reporting_active_users_aggregates__v1.set_upstream(
        bigeye__org_mozilla_ios_firefoxbeta_derived__usage_reporting_clients_daily__v1
    )

    firefox_ios_derived__usage_reporting_active_users_aggregates__v1.set_upstream(
        bigeye__org_mozilla_ios_firefoxbeta_derived__usage_reporting_clients_first_seen__v1
    )

    firefox_ios_derived__usage_reporting_active_users_aggregates__v1.set_upstream(
        bigeye__org_mozilla_ios_firefoxbeta_derived__usage_reporting_clients_last_seen__v1
    )

    focus_android_derived__usage_reporting_active_users_aggregates__v1.set_upstream(
        bigeye__org_mozilla_focus_beta_derived__usage_reporting_clients_daily__v1
    )

    focus_android_derived__usage_reporting_active_users_aggregates__v1.set_upstream(
        bigeye__org_mozilla_focus_beta_derived__usage_reporting_clients_first_seen__v1
    )

    focus_android_derived__usage_reporting_active_users_aggregates__v1.set_upstream(
        bigeye__org_mozilla_focus_beta_derived__usage_reporting_clients_last_seen__v1
    )

    focus_android_derived__usage_reporting_active_users_aggregates__v1.set_upstream(
        bigeye__org_mozilla_focus_derived__usage_reporting_clients_daily__v1
    )

    focus_android_derived__usage_reporting_active_users_aggregates__v1.set_upstream(
        bigeye__org_mozilla_focus_derived__usage_reporting_clients_first_seen__v1
    )

    focus_android_derived__usage_reporting_active_users_aggregates__v1.set_upstream(
        bigeye__org_mozilla_focus_derived__usage_reporting_clients_last_seen__v1
    )

    focus_android_derived__usage_reporting_active_users_aggregates__v1.set_upstream(
        bigeye__org_mozilla_focus_nightly_derived__usage_reporting_clients_daily__v1
    )

    focus_android_derived__usage_reporting_active_users_aggregates__v1.set_upstream(
        bigeye__org_mozilla_focus_nightly_derived__usage_reporting_clients_first_seen__v1
    )

    focus_android_derived__usage_reporting_active_users_aggregates__v1.set_upstream(
        bigeye__org_mozilla_focus_nightly_derived__usage_reporting_clients_last_seen__v1
    )

    focus_ios_derived__usage_reporting_active_users_aggregates__v1.set_upstream(
        bigeye__org_mozilla_ios_focus_derived__usage_reporting_clients_daily__v1
    )

    focus_ios_derived__usage_reporting_active_users_aggregates__v1.set_upstream(
        bigeye__org_mozilla_ios_focus_derived__usage_reporting_clients_first_seen__v1
    )

    focus_ios_derived__usage_reporting_active_users_aggregates__v1.set_upstream(
        bigeye__org_mozilla_ios_focus_derived__usage_reporting_clients_last_seen__v1
    )

    org_mozilla_fenix_derived__usage_reporting_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    org_mozilla_fenix_derived__usage_reporting_clients_first_seen__v1.set_upstream(
        bigeye__org_mozilla_fenix_derived__usage_reporting_clients_daily__v1
    )

    org_mozilla_fenix_derived__usage_reporting_clients_last_seen__v1.set_upstream(
        bigeye__org_mozilla_fenix_derived__usage_reporting_clients_daily__v1
    )

    org_mozilla_fenix_nightly_derived__usage_reporting_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    org_mozilla_fenix_nightly_derived__usage_reporting_clients_first_seen__v1.set_upstream(
        bigeye__org_mozilla_fenix_nightly_derived__usage_reporting_clients_daily__v1
    )

    org_mozilla_fenix_nightly_derived__usage_reporting_clients_last_seen__v1.set_upstream(
        bigeye__org_mozilla_fenix_nightly_derived__usage_reporting_clients_daily__v1
    )

    org_mozilla_fennec_aurora_derived__usage_reporting_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    org_mozilla_fennec_aurora_derived__usage_reporting_clients_first_seen__v1.set_upstream(
        bigeye__org_mozilla_fennec_aurora_derived__usage_reporting_clients_daily__v1
    )

    org_mozilla_fennec_aurora_derived__usage_reporting_clients_last_seen__v1.set_upstream(
        bigeye__org_mozilla_fennec_aurora_derived__usage_reporting_clients_daily__v1
    )

    org_mozilla_firefox_beta_derived__usage_reporting_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    org_mozilla_firefox_beta_derived__usage_reporting_clients_first_seen__v1.set_upstream(
        bigeye__org_mozilla_firefox_beta_derived__usage_reporting_clients_daily__v1
    )

    org_mozilla_firefox_beta_derived__usage_reporting_clients_last_seen__v1.set_upstream(
        bigeye__org_mozilla_firefox_beta_derived__usage_reporting_clients_daily__v1
    )

    org_mozilla_firefox_derived__usage_reporting_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    org_mozilla_firefox_derived__usage_reporting_clients_first_seen__v1.set_upstream(
        bigeye__org_mozilla_firefox_derived__usage_reporting_clients_daily__v1
    )

    org_mozilla_firefox_derived__usage_reporting_clients_last_seen__v1.set_upstream(
        bigeye__org_mozilla_firefox_derived__usage_reporting_clients_daily__v1
    )

    org_mozilla_focus_beta_derived__usage_reporting_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    org_mozilla_focus_beta_derived__usage_reporting_clients_first_seen__v1.set_upstream(
        bigeye__org_mozilla_focus_beta_derived__usage_reporting_clients_daily__v1
    )

    org_mozilla_focus_beta_derived__usage_reporting_clients_last_seen__v1.set_upstream(
        bigeye__org_mozilla_focus_beta_derived__usage_reporting_clients_daily__v1
    )

    org_mozilla_focus_derived__usage_reporting_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    org_mozilla_focus_derived__usage_reporting_clients_first_seen__v1.set_upstream(
        bigeye__org_mozilla_focus_derived__usage_reporting_clients_daily__v1
    )

    org_mozilla_focus_derived__usage_reporting_clients_last_seen__v1.set_upstream(
        bigeye__org_mozilla_focus_derived__usage_reporting_clients_daily__v1
    )

    org_mozilla_focus_nightly_derived__usage_reporting_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    org_mozilla_focus_nightly_derived__usage_reporting_clients_first_seen__v1.set_upstream(
        bigeye__org_mozilla_focus_nightly_derived__usage_reporting_clients_daily__v1
    )

    org_mozilla_focus_nightly_derived__usage_reporting_clients_last_seen__v1.set_upstream(
        bigeye__org_mozilla_focus_nightly_derived__usage_reporting_clients_daily__v1
    )

    org_mozilla_ios_fennec_derived__usage_reporting_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    org_mozilla_ios_fennec_derived__usage_reporting_clients_first_seen__v1.set_upstream(
        bigeye__org_mozilla_ios_fennec_derived__usage_reporting_clients_daily__v1
    )

    org_mozilla_ios_fennec_derived__usage_reporting_clients_last_seen__v1.set_upstream(
        bigeye__org_mozilla_ios_fennec_derived__usage_reporting_clients_daily__v1
    )

    org_mozilla_ios_firefox_derived__usage_reporting_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    org_mozilla_ios_firefox_derived__usage_reporting_clients_first_seen__v1.set_upstream(
        bigeye__org_mozilla_ios_firefox_derived__usage_reporting_clients_daily__v1
    )

    org_mozilla_ios_firefox_derived__usage_reporting_clients_last_seen__v1.set_upstream(
        bigeye__org_mozilla_ios_firefox_derived__usage_reporting_clients_daily__v1
    )

    org_mozilla_ios_firefoxbeta_derived__usage_reporting_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    org_mozilla_ios_firefoxbeta_derived__usage_reporting_clients_first_seen__v1.set_upstream(
        bigeye__org_mozilla_ios_firefoxbeta_derived__usage_reporting_clients_daily__v1
    )

    org_mozilla_ios_firefoxbeta_derived__usage_reporting_clients_last_seen__v1.set_upstream(
        bigeye__org_mozilla_ios_firefoxbeta_derived__usage_reporting_clients_daily__v1
    )

    org_mozilla_ios_focus_derived__usage_reporting_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    org_mozilla_ios_focus_derived__usage_reporting_clients_first_seen__v1.set_upstream(
        bigeye__org_mozilla_ios_focus_derived__usage_reporting_clients_daily__v1
    )

    org_mozilla_ios_focus_derived__usage_reporting_clients_last_seen__v1.set_upstream(
        bigeye__org_mozilla_ios_focus_derived__usage_reporting_clients_daily__v1
    )
