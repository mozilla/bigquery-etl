# Generated via https://github.com/mozilla/bigquery-etl/blob/main/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskMarker
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
import datetime
from utils.constants import ALLOWED_STATES, FAILED_STATES
from utils.gcp import bigquery_etl_query, gke_command, bigquery_dq_check

docs = """
### bqetl_glean_usage

Built from bigquery-etl repo, [`dags/bqetl_glean_usage.py`](https://github.com/mozilla/bigquery-etl/blob/main/dags/bqetl_glean_usage.py)

#### Owner

ascholtz@mozilla.com
"""


default_args = {
    "owner": "ascholtz@mozilla.com",
    "start_date": datetime.datetime(2023, 11, 20, 0, 0),
    "end_date": None,
    "email": ["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
}

tags = ["impact/tier_1", "repo/bigquery-etl"]

with DAG(
    "bqetl_glean_usage",
    default_args=default_args,
    schedule_interval="0 2 * * *",
    doc_md=docs,
    tags=tags,
) as dag:
    task_group_accounts_backend = TaskGroup("accounts_backend")

    task_group_burnham = TaskGroup("burnham")

    task_group_fenix = TaskGroup("fenix")

    task_group_firefox_desktop = TaskGroup("firefox_desktop")

    task_group_firefox_desktop_background_defaultagent = TaskGroup(
        "firefox_desktop_background_defaultagent"
    )

    task_group_firefox_desktop_background_tasks = TaskGroup(
        "firefox_desktop_background_tasks"
    )

    task_group_firefox_desktop_background_update = TaskGroup(
        "firefox_desktop_background_update"
    )

    task_group_firefox_echo_show = TaskGroup("firefox_echo_show")

    task_group_firefox_fire_tv = TaskGroup("firefox_fire_tv")

    task_group_firefox_ios = TaskGroup("firefox_ios")

    task_group_firefox_reality = TaskGroup("firefox_reality")

    task_group_firefox_reality_pc = TaskGroup("firefox_reality_pc")

    task_group_focus_android = TaskGroup("focus_android")

    task_group_focus_ios = TaskGroup("focus_ios")

    task_group_klar_android = TaskGroup("klar_android")

    task_group_klar_ios = TaskGroup("klar_ios")

    task_group_lockwise_android = TaskGroup("lockwise_android")

    task_group_lockwise_ios = TaskGroup("lockwise_ios")

    task_group_mach = TaskGroup("mach")

    task_group_monitor_cirrus = TaskGroup("monitor_cirrus")

    task_group_moso_mastodon_backend = TaskGroup("moso_mastodon_backend")

    task_group_mozilla_vpn = TaskGroup("mozilla_vpn")

    task_group_mozillavpn_cirrus = TaskGroup("mozillavpn_cirrus")

    task_group_mozphab = TaskGroup("mozphab")

    task_group_mozregression = TaskGroup("mozregression")

    task_group_pine = TaskGroup("pine")

    task_group_reference_browser = TaskGroup("reference_browser")

    accounts_backend_derived__baseline_clients_daily__v1 = bigquery_etl_query(
        task_id="accounts_backend_derived__baseline_clients_daily__v1",
        destination_table="baseline_clients_daily_v1",
        dataset_id="accounts_backend_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_accounts_backend,
    )

    accounts_backend_derived__baseline_clients_first_seen__v1 = bigquery_etl_query(
        task_id="accounts_backend_derived__baseline_clients_first_seen__v1",
        destination_table="baseline_clients_first_seen_v1",
        dataset_id="accounts_backend_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=True,
        parameters=["submission_date:DATE:{{ds}}"],
        task_group=task_group_accounts_backend,
    )

    accounts_backend_derived__baseline_clients_last_seen__v1 = bigquery_etl_query(
        task_id="accounts_backend_derived__baseline_clients_last_seen__v1",
        destination_table="baseline_clients_last_seen_v1",
        dataset_id="accounts_backend_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=True,
        task_group=task_group_accounts_backend,
    )

    accounts_backend_derived__metrics_clients_daily__v1 = bigquery_etl_query(
        task_id="accounts_backend_derived__metrics_clients_daily__v1",
        destination_table="metrics_clients_daily_v1",
        dataset_id="accounts_backend_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_accounts_backend,
    )

    burnham_derived__baseline_clients_daily__v1 = bigquery_etl_query(
        task_id="burnham_derived__baseline_clients_daily__v1",
        destination_table="baseline_clients_daily_v1",
        dataset_id="burnham_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_burnham,
    )

    burnham_derived__baseline_clients_first_seen__v1 = bigquery_etl_query(
        task_id="burnham_derived__baseline_clients_first_seen__v1",
        destination_table="baseline_clients_first_seen_v1",
        dataset_id="burnham_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=True,
        parameters=["submission_date:DATE:{{ds}}"],
        task_group=task_group_burnham,
    )

    burnham_derived__baseline_clients_last_seen__v1 = bigquery_etl_query(
        task_id="burnham_derived__baseline_clients_last_seen__v1",
        destination_table="baseline_clients_last_seen_v1",
        dataset_id="burnham_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=True,
        task_group=task_group_burnham,
    )

    burnham_derived__clients_last_seen_joined__v1 = bigquery_etl_query(
        task_id="burnham_derived__clients_last_seen_joined__v1",
        destination_table="clients_last_seen_joined_v1",
        dataset_id="burnham_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=True,
        task_group=task_group_burnham,
    )

    burnham_derived__metrics_clients_daily__v1 = bigquery_etl_query(
        task_id="burnham_derived__metrics_clients_daily__v1",
        destination_table="metrics_clients_daily_v1",
        dataset_id="burnham_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_burnham,
    )

    burnham_derived__metrics_clients_last_seen__v1 = bigquery_etl_query(
        task_id="burnham_derived__metrics_clients_last_seen__v1",
        destination_table="metrics_clients_last_seen_v1",
        dataset_id="burnham_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_burnham,
    )

    checks__warn_firefox_desktop_background_defaultagent_derived__baseline_clients_last_seen__v1 = bigquery_dq_check(
        task_id="checks__warn_firefox_desktop_background_defaultagent_derived__baseline_clients_last_seen__v1",
        source_table="baseline_clients_last_seen_v1",
        dataset_id="firefox_desktop_background_defaultagent_derived",
        project_id="moz-fx-data-shared-prod",
        is_dq_check_fail=False,
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        parameters=["submission_date:DATE:{{ds}}"],
        retries=0,
        task_group=task_group_firefox_desktop_background_defaultagent,
    )

    checks__warn_firefox_desktop_background_tasks_derived__baseline_clients_last_seen__v1 = bigquery_dq_check(
        task_id="checks__warn_firefox_desktop_background_tasks_derived__baseline_clients_last_seen__v1",
        source_table="baseline_clients_last_seen_v1",
        dataset_id="firefox_desktop_background_tasks_derived",
        project_id="moz-fx-data-shared-prod",
        is_dq_check_fail=False,
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        parameters=["submission_date:DATE:{{ds}}"],
        retries=0,
        task_group=task_group_firefox_desktop_background_tasks,
    )

    checks__warn_firefox_desktop_background_update_derived__baseline_clients_last_seen__v1 = bigquery_dq_check(
        task_id="checks__warn_firefox_desktop_background_update_derived__baseline_clients_last_seen__v1",
        source_table="baseline_clients_last_seen_v1",
        dataset_id="firefox_desktop_background_update_derived",
        project_id="moz-fx-data-shared-prod",
        is_dq_check_fail=False,
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        parameters=["submission_date:DATE:{{ds}}"],
        retries=0,
        task_group=task_group_firefox_desktop_background_update,
    )

    checks__warn_firefox_desktop_derived__baseline_clients_last_seen__v1 = bigquery_dq_check(
        task_id="checks__warn_firefox_desktop_derived__baseline_clients_last_seen__v1",
        source_table="baseline_clients_last_seen_v1",
        dataset_id="firefox_desktop_derived",
        project_id="moz-fx-data-shared-prod",
        is_dq_check_fail=False,
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        parameters=["submission_date:DATE:{{ds}}"],
        retries=0,
        task_group=task_group_firefox_desktop,
    )

    checks__warn_org_mozilla_connect_firefox_derived__baseline_clients_last_seen__v1 = bigquery_dq_check(
        task_id="checks__warn_org_mozilla_connect_firefox_derived__baseline_clients_last_seen__v1",
        source_table="baseline_clients_last_seen_v1",
        dataset_id="org_mozilla_connect_firefox_derived",
        project_id="moz-fx-data-shared-prod",
        is_dq_check_fail=False,
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        parameters=["submission_date:DATE:{{ds}}"],
        retries=0,
        task_group=task_group_firefox_echo_show,
    )

    checks__warn_org_mozilla_fenix_derived__baseline_clients_last_seen__v1 = bigquery_dq_check(
        task_id="checks__warn_org_mozilla_fenix_derived__baseline_clients_last_seen__v1",
        source_table="baseline_clients_last_seen_v1",
        dataset_id="org_mozilla_fenix_derived",
        project_id="moz-fx-data-shared-prod",
        is_dq_check_fail=False,
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        parameters=["submission_date:DATE:{{ds}}"],
        retries=0,
        task_group=task_group_fenix,
    )

    checks__warn_org_mozilla_fenix_nightly_derived__baseline_clients_last_seen__v1 = bigquery_dq_check(
        task_id="checks__warn_org_mozilla_fenix_nightly_derived__baseline_clients_last_seen__v1",
        source_table="baseline_clients_last_seen_v1",
        dataset_id="org_mozilla_fenix_nightly_derived",
        project_id="moz-fx-data-shared-prod",
        is_dq_check_fail=False,
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        parameters=["submission_date:DATE:{{ds}}"],
        retries=0,
        task_group=task_group_fenix,
    )

    checks__warn_org_mozilla_fennec_aurora_derived__baseline_clients_last_seen__v1 = bigquery_dq_check(
        task_id="checks__warn_org_mozilla_fennec_aurora_derived__baseline_clients_last_seen__v1",
        source_table="baseline_clients_last_seen_v1",
        dataset_id="org_mozilla_fennec_aurora_derived",
        project_id="moz-fx-data-shared-prod",
        is_dq_check_fail=False,
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        parameters=["submission_date:DATE:{{ds}}"],
        retries=0,
        task_group=task_group_fenix,
    )

    checks__warn_org_mozilla_firefox_beta_derived__baseline_clients_last_seen__v1 = bigquery_dq_check(
        task_id="checks__warn_org_mozilla_firefox_beta_derived__baseline_clients_last_seen__v1",
        source_table="baseline_clients_last_seen_v1",
        dataset_id="org_mozilla_firefox_beta_derived",
        project_id="moz-fx-data-shared-prod",
        is_dq_check_fail=False,
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        parameters=["submission_date:DATE:{{ds}}"],
        retries=0,
        task_group=task_group_fenix,
    )

    checks__warn_org_mozilla_firefox_derived__baseline_clients_last_seen__v1 = bigquery_dq_check(
        task_id="checks__warn_org_mozilla_firefox_derived__baseline_clients_last_seen__v1",
        source_table="baseline_clients_last_seen_v1",
        dataset_id="org_mozilla_firefox_derived",
        project_id="moz-fx-data-shared-prod",
        is_dq_check_fail=False,
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        parameters=["submission_date:DATE:{{ds}}"],
        retries=0,
        task_group=task_group_fenix,
    )

    checks__warn_org_mozilla_focus_beta_derived__baseline_clients_last_seen__v1 = bigquery_dq_check(
        task_id="checks__warn_org_mozilla_focus_beta_derived__baseline_clients_last_seen__v1",
        source_table="baseline_clients_last_seen_v1",
        dataset_id="org_mozilla_focus_beta_derived",
        project_id="moz-fx-data-shared-prod",
        is_dq_check_fail=False,
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        parameters=["submission_date:DATE:{{ds}}"],
        retries=0,
        task_group=task_group_focus_android,
    )

    checks__warn_org_mozilla_focus_derived__baseline_clients_last_seen__v1 = bigquery_dq_check(
        task_id="checks__warn_org_mozilla_focus_derived__baseline_clients_last_seen__v1",
        source_table="baseline_clients_last_seen_v1",
        dataset_id="org_mozilla_focus_derived",
        project_id="moz-fx-data-shared-prod",
        is_dq_check_fail=False,
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        parameters=["submission_date:DATE:{{ds}}"],
        retries=0,
        task_group=task_group_focus_android,
    )

    checks__warn_org_mozilla_focus_nightly_derived__baseline_clients_last_seen__v1 = bigquery_dq_check(
        task_id="checks__warn_org_mozilla_focus_nightly_derived__baseline_clients_last_seen__v1",
        source_table="baseline_clients_last_seen_v1",
        dataset_id="org_mozilla_focus_nightly_derived",
        project_id="moz-fx-data-shared-prod",
        is_dq_check_fail=False,
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        parameters=["submission_date:DATE:{{ds}}"],
        retries=0,
        task_group=task_group_focus_android,
    )

    checks__warn_org_mozilla_ios_fennec_derived__baseline_clients_last_seen__v1 = bigquery_dq_check(
        task_id="checks__warn_org_mozilla_ios_fennec_derived__baseline_clients_last_seen__v1",
        source_table="baseline_clients_last_seen_v1",
        dataset_id="org_mozilla_ios_fennec_derived",
        project_id="moz-fx-data-shared-prod",
        is_dq_check_fail=False,
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        parameters=["submission_date:DATE:{{ds}}"],
        retries=0,
        task_group=task_group_firefox_ios,
    )

    checks__warn_org_mozilla_ios_firefox_derived__baseline_clients_last_seen__v1 = bigquery_dq_check(
        task_id="checks__warn_org_mozilla_ios_firefox_derived__baseline_clients_last_seen__v1",
        source_table="baseline_clients_last_seen_v1",
        dataset_id="org_mozilla_ios_firefox_derived",
        project_id="moz-fx-data-shared-prod",
        is_dq_check_fail=False,
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        parameters=["submission_date:DATE:{{ds}}"],
        retries=0,
        task_group=task_group_firefox_ios,
    )

    checks__warn_org_mozilla_ios_firefoxbeta_derived__baseline_clients_last_seen__v1 = bigquery_dq_check(
        task_id="checks__warn_org_mozilla_ios_firefoxbeta_derived__baseline_clients_last_seen__v1",
        source_table="baseline_clients_last_seen_v1",
        dataset_id="org_mozilla_ios_firefoxbeta_derived",
        project_id="moz-fx-data-shared-prod",
        is_dq_check_fail=False,
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        parameters=["submission_date:DATE:{{ds}}"],
        retries=0,
        task_group=task_group_firefox_ios,
    )

    checks__warn_org_mozilla_ios_focus_derived__baseline_clients_last_seen__v1 = bigquery_dq_check(
        task_id="checks__warn_org_mozilla_ios_focus_derived__baseline_clients_last_seen__v1",
        source_table="baseline_clients_last_seen_v1",
        dataset_id="org_mozilla_ios_focus_derived",
        project_id="moz-fx-data-shared-prod",
        is_dq_check_fail=False,
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        parameters=["submission_date:DATE:{{ds}}"],
        retries=0,
        task_group=task_group_focus_ios,
    )

    checks__warn_org_mozilla_ios_klar_derived__baseline_clients_last_seen__v1 = bigquery_dq_check(
        task_id="checks__warn_org_mozilla_ios_klar_derived__baseline_clients_last_seen__v1",
        source_table="baseline_clients_last_seen_v1",
        dataset_id="org_mozilla_ios_klar_derived",
        project_id="moz-fx-data-shared-prod",
        is_dq_check_fail=False,
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        parameters=["submission_date:DATE:{{ds}}"],
        retries=0,
        task_group=task_group_klar_ios,
    )

    checks__warn_org_mozilla_ios_lockbox_derived__baseline_clients_last_seen__v1 = bigquery_dq_check(
        task_id="checks__warn_org_mozilla_ios_lockbox_derived__baseline_clients_last_seen__v1",
        source_table="baseline_clients_last_seen_v1",
        dataset_id="org_mozilla_ios_lockbox_derived",
        project_id="moz-fx-data-shared-prod",
        is_dq_check_fail=False,
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        parameters=["submission_date:DATE:{{ds}}"],
        retries=0,
        task_group=task_group_lockwise_ios,
    )

    checks__warn_org_mozilla_klar_derived__baseline_clients_last_seen__v1 = bigquery_dq_check(
        task_id="checks__warn_org_mozilla_klar_derived__baseline_clients_last_seen__v1",
        source_table="baseline_clients_last_seen_v1",
        dataset_id="org_mozilla_klar_derived",
        project_id="moz-fx-data-shared-prod",
        is_dq_check_fail=False,
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        parameters=["submission_date:DATE:{{ds}}"],
        retries=0,
        task_group=task_group_klar_android,
    )

    checks__warn_org_mozilla_reference_browser_derived__baseline_clients_last_seen__v1 = bigquery_dq_check(
        task_id="checks__warn_org_mozilla_reference_browser_derived__baseline_clients_last_seen__v1",
        source_table="baseline_clients_last_seen_v1",
        dataset_id="org_mozilla_reference_browser_derived",
        project_id="moz-fx-data-shared-prod",
        is_dq_check_fail=False,
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        parameters=["submission_date:DATE:{{ds}}"],
        retries=0,
        task_group=task_group_reference_browser,
    )

    checks__warn_org_mozilla_vrbrowser_derived__baseline_clients_last_seen__v1 = bigquery_dq_check(
        task_id="checks__warn_org_mozilla_vrbrowser_derived__baseline_clients_last_seen__v1",
        source_table="baseline_clients_last_seen_v1",
        dataset_id="org_mozilla_vrbrowser_derived",
        project_id="moz-fx-data-shared-prod",
        is_dq_check_fail=False,
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        parameters=["submission_date:DATE:{{ds}}"],
        retries=0,
        task_group=task_group_firefox_reality,
    )

    checks__warn_pine_derived__baseline_clients_last_seen__v1 = bigquery_dq_check(
        task_id="checks__warn_pine_derived__baseline_clients_last_seen__v1",
        source_table="baseline_clients_last_seen_v1",
        dataset_id="pine_derived",
        project_id="moz-fx-data-shared-prod",
        is_dq_check_fail=False,
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        parameters=["submission_date:DATE:{{ds}}"],
        retries=0,
        task_group=task_group_pine,
    )

    fenix_derived__clients_last_seen_joined__v1 = bigquery_etl_query(
        task_id="fenix_derived__clients_last_seen_joined__v1",
        destination_table="clients_last_seen_joined_v1",
        dataset_id="fenix_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=True,
        task_group=task_group_fenix,
    )

    with TaskGroup(
        "fenix_derived__clients_last_seen_joined__v1_external",
        parent_group=task_group_fenix,
    ) as fenix_derived__clients_last_seen_joined__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_analytics_aggregations__wait_for_fenix_derived__clients_last_seen_joined__v1",
            external_dag_id="bqetl_analytics_aggregations",
            external_task_id="wait_for_fenix_derived__clients_last_seen_joined__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=78300)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_kpis_shredder__wait_for_fenix_derived__clients_last_seen_joined__v1",
            external_dag_id="bqetl_kpis_shredder",
            external_task_id="wait_for_fenix_derived__clients_last_seen_joined__v1",
        )

        ExternalTaskMarker(
            task_id="bqetl_unified__wait_for_fenix_derived__clients_last_seen_joined__v1",
            external_dag_id="bqetl_unified",
            external_task_id="wait_for_fenix_derived__clients_last_seen_joined__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=82800)).isoformat() }}",
        )

        fenix_derived__clients_last_seen_joined__v1_external.set_upstream(
            fenix_derived__clients_last_seen_joined__v1
        )

    fenix_derived__metrics_clients_daily__v1 = bigquery_etl_query(
        task_id="fenix_derived__metrics_clients_daily__v1",
        destination_table="metrics_clients_daily_v1",
        dataset_id="fenix_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_fenix,
    )

    fenix_derived__metrics_clients_last_seen__v1 = bigquery_etl_query(
        task_id="fenix_derived__metrics_clients_last_seen__v1",
        destination_table="metrics_clients_last_seen_v1",
        dataset_id="fenix_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_fenix,
    )

    firefox_desktop_background_defaultagent_derived__baseline_clients_daily__v1 = bigquery_etl_query(
        task_id="firefox_desktop_background_defaultagent_derived__baseline_clients_daily__v1",
        destination_table="baseline_clients_daily_v1",
        dataset_id="firefox_desktop_background_defaultagent_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_firefox_desktop_background_defaultagent,
    )

    firefox_desktop_background_defaultagent_derived__baseline_clients_first_seen__v1 = bigquery_etl_query(
        task_id="firefox_desktop_background_defaultagent_derived__baseline_clients_first_seen__v1",
        destination_table="baseline_clients_first_seen_v1",
        dataset_id="firefox_desktop_background_defaultagent_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=True,
        parameters=["submission_date:DATE:{{ds}}"],
        task_group=task_group_firefox_desktop_background_defaultagent,
    )

    firefox_desktop_background_defaultagent_derived__baseline_clients_last_seen__v1 = bigquery_etl_query(
        task_id="firefox_desktop_background_defaultagent_derived__baseline_clients_last_seen__v1",
        destination_table="baseline_clients_last_seen_v1",
        dataset_id="firefox_desktop_background_defaultagent_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=True,
        task_group=task_group_firefox_desktop_background_defaultagent,
    )

    firefox_desktop_background_defaultagent_derived__metrics_clients_daily__v1 = bigquery_etl_query(
        task_id="firefox_desktop_background_defaultagent_derived__metrics_clients_daily__v1",
        destination_table="metrics_clients_daily_v1",
        dataset_id="firefox_desktop_background_defaultagent_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_firefox_desktop_background_defaultagent,
    )

    firefox_desktop_background_tasks_derived__baseline_clients_daily__v1 = bigquery_etl_query(
        task_id="firefox_desktop_background_tasks_derived__baseline_clients_daily__v1",
        destination_table="baseline_clients_daily_v1",
        dataset_id="firefox_desktop_background_tasks_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_firefox_desktop_background_tasks,
    )

    firefox_desktop_background_tasks_derived__baseline_clients_first_seen__v1 = bigquery_etl_query(
        task_id="firefox_desktop_background_tasks_derived__baseline_clients_first_seen__v1",
        destination_table="baseline_clients_first_seen_v1",
        dataset_id="firefox_desktop_background_tasks_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=True,
        parameters=["submission_date:DATE:{{ds}}"],
        task_group=task_group_firefox_desktop_background_tasks,
    )

    firefox_desktop_background_tasks_derived__baseline_clients_last_seen__v1 = bigquery_etl_query(
        task_id="firefox_desktop_background_tasks_derived__baseline_clients_last_seen__v1",
        destination_table="baseline_clients_last_seen_v1",
        dataset_id="firefox_desktop_background_tasks_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=True,
        task_group=task_group_firefox_desktop_background_tasks,
    )

    firefox_desktop_background_tasks_derived__metrics_clients_daily__v1 = bigquery_etl_query(
        task_id="firefox_desktop_background_tasks_derived__metrics_clients_daily__v1",
        destination_table="metrics_clients_daily_v1",
        dataset_id="firefox_desktop_background_tasks_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_firefox_desktop_background_tasks,
    )

    firefox_desktop_background_update_derived__baseline_clients_daily__v1 = bigquery_etl_query(
        task_id="firefox_desktop_background_update_derived__baseline_clients_daily__v1",
        destination_table="baseline_clients_daily_v1",
        dataset_id="firefox_desktop_background_update_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_firefox_desktop_background_update,
    )

    firefox_desktop_background_update_derived__baseline_clients_first_seen__v1 = bigquery_etl_query(
        task_id="firefox_desktop_background_update_derived__baseline_clients_first_seen__v1",
        destination_table="baseline_clients_first_seen_v1",
        dataset_id="firefox_desktop_background_update_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=True,
        parameters=["submission_date:DATE:{{ds}}"],
        task_group=task_group_firefox_desktop_background_update,
    )

    firefox_desktop_background_update_derived__baseline_clients_last_seen__v1 = bigquery_etl_query(
        task_id="firefox_desktop_background_update_derived__baseline_clients_last_seen__v1",
        destination_table="baseline_clients_last_seen_v1",
        dataset_id="firefox_desktop_background_update_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=True,
        task_group=task_group_firefox_desktop_background_update,
    )

    firefox_desktop_background_update_derived__clients_last_seen_joined__v1 = bigquery_etl_query(
        task_id="firefox_desktop_background_update_derived__clients_last_seen_joined__v1",
        destination_table="clients_last_seen_joined_v1",
        dataset_id="firefox_desktop_background_update_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=True,
        task_group=task_group_firefox_desktop_background_update,
    )

    firefox_desktop_background_update_derived__metrics_clients_daily__v1 = bigquery_etl_query(
        task_id="firefox_desktop_background_update_derived__metrics_clients_daily__v1",
        destination_table="metrics_clients_daily_v1",
        dataset_id="firefox_desktop_background_update_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_firefox_desktop_background_update,
    )

    firefox_desktop_background_update_derived__metrics_clients_last_seen__v1 = bigquery_etl_query(
        task_id="firefox_desktop_background_update_derived__metrics_clients_last_seen__v1",
        destination_table="metrics_clients_last_seen_v1",
        dataset_id="firefox_desktop_background_update_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_firefox_desktop_background_update,
    )

    firefox_desktop_derived__baseline_clients_daily__v1 = bigquery_etl_query(
        task_id="firefox_desktop_derived__baseline_clients_daily__v1",
        destination_table="baseline_clients_daily_v1",
        dataset_id="firefox_desktop_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_firefox_desktop,
    )

    firefox_desktop_derived__baseline_clients_first_seen__v1 = bigquery_etl_query(
        task_id="firefox_desktop_derived__baseline_clients_first_seen__v1",
        destination_table="baseline_clients_first_seen_v1",
        dataset_id="firefox_desktop_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=True,
        parameters=["submission_date:DATE:{{ds}}"],
        task_group=task_group_firefox_desktop,
    )

    firefox_desktop_derived__baseline_clients_last_seen__v1 = bigquery_etl_query(
        task_id="firefox_desktop_derived__baseline_clients_last_seen__v1",
        destination_table="baseline_clients_last_seen_v1",
        dataset_id="firefox_desktop_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=True,
        task_group=task_group_firefox_desktop,
    )

    firefox_desktop_derived__clients_last_seen_joined__v1 = bigquery_etl_query(
        task_id="firefox_desktop_derived__clients_last_seen_joined__v1",
        destination_table="clients_last_seen_joined_v1",
        dataset_id="firefox_desktop_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=True,
        task_group=task_group_firefox_desktop,
    )

    firefox_desktop_derived__metrics_clients_daily__v1 = bigquery_etl_query(
        task_id="firefox_desktop_derived__metrics_clients_daily__v1",
        destination_table="metrics_clients_daily_v1",
        dataset_id="firefox_desktop_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_firefox_desktop,
    )

    firefox_desktop_derived__metrics_clients_last_seen__v1 = bigquery_etl_query(
        task_id="firefox_desktop_derived__metrics_clients_last_seen__v1",
        destination_table="metrics_clients_last_seen_v1",
        dataset_id="firefox_desktop_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_firefox_desktop,
    )

    firefox_echo_show_derived__clients_last_seen_joined__v1 = bigquery_etl_query(
        task_id="firefox_echo_show_derived__clients_last_seen_joined__v1",
        destination_table="clients_last_seen_joined_v1",
        dataset_id="firefox_echo_show_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=True,
        task_group=task_group_firefox_echo_show,
    )

    firefox_echo_show_derived__metrics_clients_daily__v1 = bigquery_etl_query(
        task_id="firefox_echo_show_derived__metrics_clients_daily__v1",
        destination_table="metrics_clients_daily_v1",
        dataset_id="firefox_echo_show_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_firefox_echo_show,
    )

    firefox_echo_show_derived__metrics_clients_last_seen__v1 = bigquery_etl_query(
        task_id="firefox_echo_show_derived__metrics_clients_last_seen__v1",
        destination_table="metrics_clients_last_seen_v1",
        dataset_id="firefox_echo_show_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_firefox_echo_show,
    )

    firefox_fire_tv_derived__clients_last_seen_joined__v1 = bigquery_etl_query(
        task_id="firefox_fire_tv_derived__clients_last_seen_joined__v1",
        destination_table="clients_last_seen_joined_v1",
        dataset_id="firefox_fire_tv_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=True,
        task_group=task_group_firefox_fire_tv,
    )

    firefox_fire_tv_derived__metrics_clients_daily__v1 = bigquery_etl_query(
        task_id="firefox_fire_tv_derived__metrics_clients_daily__v1",
        destination_table="metrics_clients_daily_v1",
        dataset_id="firefox_fire_tv_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_firefox_fire_tv,
    )

    firefox_fire_tv_derived__metrics_clients_last_seen__v1 = bigquery_etl_query(
        task_id="firefox_fire_tv_derived__metrics_clients_last_seen__v1",
        destination_table="metrics_clients_last_seen_v1",
        dataset_id="firefox_fire_tv_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_firefox_fire_tv,
    )

    firefox_ios_derived__clients_last_seen_joined__v1 = bigquery_etl_query(
        task_id="firefox_ios_derived__clients_last_seen_joined__v1",
        destination_table="clients_last_seen_joined_v1",
        dataset_id="firefox_ios_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=True,
        task_group=task_group_firefox_ios,
    )

    with TaskGroup(
        "firefox_ios_derived__clients_last_seen_joined__v1_external",
        parent_group=task_group_firefox_ios,
    ) as firefox_ios_derived__clients_last_seen_joined__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_analytics_aggregations__wait_for_firefox_ios_derived__clients_last_seen_joined__v1",
            external_dag_id="bqetl_analytics_aggregations",
            external_task_id="wait_for_firefox_ios_derived__clients_last_seen_joined__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=78300)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_kpis_shredder__wait_for_firefox_ios_derived__clients_last_seen_joined__v1",
            external_dag_id="bqetl_kpis_shredder",
            external_task_id="wait_for_firefox_ios_derived__clients_last_seen_joined__v1",
        )

        ExternalTaskMarker(
            task_id="bqetl_unified__wait_for_firefox_ios_derived__clients_last_seen_joined__v1",
            external_dag_id="bqetl_unified",
            external_task_id="wait_for_firefox_ios_derived__clients_last_seen_joined__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=82800)).isoformat() }}",
        )

        firefox_ios_derived__clients_last_seen_joined__v1_external.set_upstream(
            firefox_ios_derived__clients_last_seen_joined__v1
        )

    firefox_ios_derived__metrics_clients_daily__v1 = bigquery_etl_query(
        task_id="firefox_ios_derived__metrics_clients_daily__v1",
        destination_table="metrics_clients_daily_v1",
        dataset_id="firefox_ios_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_firefox_ios,
    )

    firefox_ios_derived__metrics_clients_last_seen__v1 = bigquery_etl_query(
        task_id="firefox_ios_derived__metrics_clients_last_seen__v1",
        destination_table="metrics_clients_last_seen_v1",
        dataset_id="firefox_ios_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_firefox_ios,
    )

    firefox_reality_derived__clients_last_seen_joined__v1 = bigquery_etl_query(
        task_id="firefox_reality_derived__clients_last_seen_joined__v1",
        destination_table="clients_last_seen_joined_v1",
        dataset_id="firefox_reality_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=True,
        task_group=task_group_firefox_reality,
    )

    firefox_reality_derived__metrics_clients_daily__v1 = bigquery_etl_query(
        task_id="firefox_reality_derived__metrics_clients_daily__v1",
        destination_table="metrics_clients_daily_v1",
        dataset_id="firefox_reality_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_firefox_reality,
    )

    firefox_reality_derived__metrics_clients_last_seen__v1 = bigquery_etl_query(
        task_id="firefox_reality_derived__metrics_clients_last_seen__v1",
        destination_table="metrics_clients_last_seen_v1",
        dataset_id="firefox_reality_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_firefox_reality,
    )

    firefox_reality_pc_derived__clients_last_seen_joined__v1 = bigquery_etl_query(
        task_id="firefox_reality_pc_derived__clients_last_seen_joined__v1",
        destination_table="clients_last_seen_joined_v1",
        dataset_id="firefox_reality_pc_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=True,
        task_group=task_group_firefox_reality_pc,
    )

    firefox_reality_pc_derived__metrics_clients_daily__v1 = bigquery_etl_query(
        task_id="firefox_reality_pc_derived__metrics_clients_daily__v1",
        destination_table="metrics_clients_daily_v1",
        dataset_id="firefox_reality_pc_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_firefox_reality_pc,
    )

    firefox_reality_pc_derived__metrics_clients_last_seen__v1 = bigquery_etl_query(
        task_id="firefox_reality_pc_derived__metrics_clients_last_seen__v1",
        destination_table="metrics_clients_last_seen_v1",
        dataset_id="firefox_reality_pc_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_firefox_reality_pc,
    )

    focus_android_derived__clients_last_seen_joined__v1 = bigquery_etl_query(
        task_id="focus_android_derived__clients_last_seen_joined__v1",
        destination_table="clients_last_seen_joined_v1",
        dataset_id="focus_android_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=True,
        task_group=task_group_focus_android,
    )

    with TaskGroup(
        "focus_android_derived__clients_last_seen_joined__v1_external",
        parent_group=task_group_focus_android,
    ) as focus_android_derived__clients_last_seen_joined__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_analytics_aggregations__wait_for_focus_android_derived__clients_last_seen_joined__v1",
            external_dag_id="bqetl_analytics_aggregations",
            external_task_id="wait_for_focus_android_derived__clients_last_seen_joined__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=78300)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_unified__wait_for_focus_android_derived__clients_last_seen_joined__v1",
            external_dag_id="bqetl_unified",
            external_task_id="wait_for_focus_android_derived__clients_last_seen_joined__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=82800)).isoformat() }}",
        )

        focus_android_derived__clients_last_seen_joined__v1_external.set_upstream(
            focus_android_derived__clients_last_seen_joined__v1
        )

    focus_android_derived__metrics_clients_daily__v1 = bigquery_etl_query(
        task_id="focus_android_derived__metrics_clients_daily__v1",
        destination_table="metrics_clients_daily_v1",
        dataset_id="focus_android_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_focus_android,
    )

    focus_android_derived__metrics_clients_last_seen__v1 = bigquery_etl_query(
        task_id="focus_android_derived__metrics_clients_last_seen__v1",
        destination_table="metrics_clients_last_seen_v1",
        dataset_id="focus_android_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_focus_android,
    )

    focus_ios_derived__clients_last_seen_joined__v1 = bigquery_etl_query(
        task_id="focus_ios_derived__clients_last_seen_joined__v1",
        destination_table="clients_last_seen_joined_v1",
        dataset_id="focus_ios_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=True,
        task_group=task_group_focus_ios,
    )

    with TaskGroup(
        "focus_ios_derived__clients_last_seen_joined__v1_external",
        parent_group=task_group_focus_ios,
    ) as focus_ios_derived__clients_last_seen_joined__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_analytics_aggregations__wait_for_focus_ios_derived__clients_last_seen_joined__v1",
            external_dag_id="bqetl_analytics_aggregations",
            external_task_id="wait_for_focus_ios_derived__clients_last_seen_joined__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=78300)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_kpis_shredder__wait_for_focus_ios_derived__clients_last_seen_joined__v1",
            external_dag_id="bqetl_kpis_shredder",
            external_task_id="wait_for_focus_ios_derived__clients_last_seen_joined__v1",
        )

        ExternalTaskMarker(
            task_id="bqetl_unified__wait_for_focus_ios_derived__clients_last_seen_joined__v1",
            external_dag_id="bqetl_unified",
            external_task_id="wait_for_focus_ios_derived__clients_last_seen_joined__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=82800)).isoformat() }}",
        )

        focus_ios_derived__clients_last_seen_joined__v1_external.set_upstream(
            focus_ios_derived__clients_last_seen_joined__v1
        )

    focus_ios_derived__metrics_clients_daily__v1 = bigquery_etl_query(
        task_id="focus_ios_derived__metrics_clients_daily__v1",
        destination_table="metrics_clients_daily_v1",
        dataset_id="focus_ios_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_focus_ios,
    )

    focus_ios_derived__metrics_clients_last_seen__v1 = bigquery_etl_query(
        task_id="focus_ios_derived__metrics_clients_last_seen__v1",
        destination_table="metrics_clients_last_seen_v1",
        dataset_id="focus_ios_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_focus_ios,
    )

    klar_android_derived__clients_last_seen_joined__v1 = bigquery_etl_query(
        task_id="klar_android_derived__clients_last_seen_joined__v1",
        destination_table="clients_last_seen_joined_v1",
        dataset_id="klar_android_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=True,
        task_group=task_group_klar_android,
    )

    klar_android_derived__metrics_clients_daily__v1 = bigquery_etl_query(
        task_id="klar_android_derived__metrics_clients_daily__v1",
        destination_table="metrics_clients_daily_v1",
        dataset_id="klar_android_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_klar_android,
    )

    klar_android_derived__metrics_clients_last_seen__v1 = bigquery_etl_query(
        task_id="klar_android_derived__metrics_clients_last_seen__v1",
        destination_table="metrics_clients_last_seen_v1",
        dataset_id="klar_android_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_klar_android,
    )

    klar_ios_derived__clients_last_seen_joined__v1 = bigquery_etl_query(
        task_id="klar_ios_derived__clients_last_seen_joined__v1",
        destination_table="clients_last_seen_joined_v1",
        dataset_id="klar_ios_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=True,
        task_group=task_group_klar_ios,
    )

    with TaskGroup(
        "klar_ios_derived__clients_last_seen_joined__v1_external",
        parent_group=task_group_klar_ios,
    ) as klar_ios_derived__clients_last_seen_joined__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_analytics_aggregations__wait_for_klar_ios_derived__clients_last_seen_joined__v1",
            external_dag_id="bqetl_analytics_aggregations",
            external_task_id="wait_for_klar_ios_derived__clients_last_seen_joined__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=78300)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_kpis_shredder__wait_for_klar_ios_derived__clients_last_seen_joined__v1",
            external_dag_id="bqetl_kpis_shredder",
            external_task_id="wait_for_klar_ios_derived__clients_last_seen_joined__v1",
        )

        ExternalTaskMarker(
            task_id="bqetl_unified__wait_for_klar_ios_derived__clients_last_seen_joined__v1",
            external_dag_id="bqetl_unified",
            external_task_id="wait_for_klar_ios_derived__clients_last_seen_joined__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=82800)).isoformat() }}",
        )

        klar_ios_derived__clients_last_seen_joined__v1_external.set_upstream(
            klar_ios_derived__clients_last_seen_joined__v1
        )

    klar_ios_derived__metrics_clients_daily__v1 = bigquery_etl_query(
        task_id="klar_ios_derived__metrics_clients_daily__v1",
        destination_table="metrics_clients_daily_v1",
        dataset_id="klar_ios_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_klar_ios,
    )

    klar_ios_derived__metrics_clients_last_seen__v1 = bigquery_etl_query(
        task_id="klar_ios_derived__metrics_clients_last_seen__v1",
        destination_table="metrics_clients_last_seen_v1",
        dataset_id="klar_ios_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_klar_ios,
    )

    lockwise_android_derived__clients_last_seen_joined__v1 = bigquery_etl_query(
        task_id="lockwise_android_derived__clients_last_seen_joined__v1",
        destination_table="clients_last_seen_joined_v1",
        dataset_id="lockwise_android_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=True,
        task_group=task_group_lockwise_android,
    )

    lockwise_android_derived__metrics_clients_daily__v1 = bigquery_etl_query(
        task_id="lockwise_android_derived__metrics_clients_daily__v1",
        destination_table="metrics_clients_daily_v1",
        dataset_id="lockwise_android_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_lockwise_android,
    )

    lockwise_android_derived__metrics_clients_last_seen__v1 = bigquery_etl_query(
        task_id="lockwise_android_derived__metrics_clients_last_seen__v1",
        destination_table="metrics_clients_last_seen_v1",
        dataset_id="lockwise_android_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_lockwise_android,
    )

    lockwise_ios_derived__clients_last_seen_joined__v1 = bigquery_etl_query(
        task_id="lockwise_ios_derived__clients_last_seen_joined__v1",
        destination_table="clients_last_seen_joined_v1",
        dataset_id="lockwise_ios_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=True,
        task_group=task_group_lockwise_ios,
    )

    lockwise_ios_derived__metrics_clients_daily__v1 = bigquery_etl_query(
        task_id="lockwise_ios_derived__metrics_clients_daily__v1",
        destination_table="metrics_clients_daily_v1",
        dataset_id="lockwise_ios_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_lockwise_ios,
    )

    lockwise_ios_derived__metrics_clients_last_seen__v1 = bigquery_etl_query(
        task_id="lockwise_ios_derived__metrics_clients_last_seen__v1",
        destination_table="metrics_clients_last_seen_v1",
        dataset_id="lockwise_ios_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_lockwise_ios,
    )

    mach_derived__clients_last_seen_joined__v1 = bigquery_etl_query(
        task_id="mach_derived__clients_last_seen_joined__v1",
        destination_table="clients_last_seen_joined_v1",
        dataset_id="mach_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=True,
        task_group=task_group_mach,
    )

    mach_derived__metrics_clients_daily__v1 = bigquery_etl_query(
        task_id="mach_derived__metrics_clients_daily__v1",
        destination_table="metrics_clients_daily_v1",
        dataset_id="mach_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_mach,
    )

    mach_derived__metrics_clients_last_seen__v1 = bigquery_etl_query(
        task_id="mach_derived__metrics_clients_last_seen__v1",
        destination_table="metrics_clients_last_seen_v1",
        dataset_id="mach_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_mach,
    )

    monitor_cirrus_derived__baseline_clients_daily__v1 = bigquery_etl_query(
        task_id="monitor_cirrus_derived__baseline_clients_daily__v1",
        destination_table="baseline_clients_daily_v1",
        dataset_id="monitor_cirrus_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_monitor_cirrus,
    )

    monitor_cirrus_derived__baseline_clients_first_seen__v1 = bigquery_etl_query(
        task_id="monitor_cirrus_derived__baseline_clients_first_seen__v1",
        destination_table="baseline_clients_first_seen_v1",
        dataset_id="monitor_cirrus_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=True,
        parameters=["submission_date:DATE:{{ds}}"],
        task_group=task_group_monitor_cirrus,
    )

    monitor_cirrus_derived__baseline_clients_last_seen__v1 = bigquery_etl_query(
        task_id="monitor_cirrus_derived__baseline_clients_last_seen__v1",
        destination_table="baseline_clients_last_seen_v1",
        dataset_id="monitor_cirrus_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=True,
        task_group=task_group_monitor_cirrus,
    )

    monitor_cirrus_derived__metrics_clients_daily__v1 = bigquery_etl_query(
        task_id="monitor_cirrus_derived__metrics_clients_daily__v1",
        destination_table="metrics_clients_daily_v1",
        dataset_id="monitor_cirrus_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_monitor_cirrus,
    )

    moso_mastodon_backend_derived__baseline_clients_daily__v1 = bigquery_etl_query(
        task_id="moso_mastodon_backend_derived__baseline_clients_daily__v1",
        destination_table="baseline_clients_daily_v1",
        dataset_id="moso_mastodon_backend_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_moso_mastodon_backend,
    )

    moso_mastodon_backend_derived__baseline_clients_first_seen__v1 = bigquery_etl_query(
        task_id="moso_mastodon_backend_derived__baseline_clients_first_seen__v1",
        destination_table="baseline_clients_first_seen_v1",
        dataset_id="moso_mastodon_backend_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=True,
        parameters=["submission_date:DATE:{{ds}}"],
        task_group=task_group_moso_mastodon_backend,
    )

    moso_mastodon_backend_derived__baseline_clients_last_seen__v1 = bigquery_etl_query(
        task_id="moso_mastodon_backend_derived__baseline_clients_last_seen__v1",
        destination_table="baseline_clients_last_seen_v1",
        dataset_id="moso_mastodon_backend_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=True,
        task_group=task_group_moso_mastodon_backend,
    )

    moso_mastodon_backend_derived__metrics_clients_daily__v1 = bigquery_etl_query(
        task_id="moso_mastodon_backend_derived__metrics_clients_daily__v1",
        destination_table="metrics_clients_daily_v1",
        dataset_id="moso_mastodon_backend_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_moso_mastodon_backend,
    )

    mozilla_lockbox_derived__baseline_clients_daily__v1 = bigquery_etl_query(
        task_id="mozilla_lockbox_derived__baseline_clients_daily__v1",
        destination_table="baseline_clients_daily_v1",
        dataset_id="mozilla_lockbox_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_lockwise_android,
    )

    mozilla_lockbox_derived__baseline_clients_first_seen__v1 = bigquery_etl_query(
        task_id="mozilla_lockbox_derived__baseline_clients_first_seen__v1",
        destination_table="baseline_clients_first_seen_v1",
        dataset_id="mozilla_lockbox_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=True,
        parameters=["submission_date:DATE:{{ds}}"],
        task_group=task_group_lockwise_android,
    )

    mozilla_lockbox_derived__baseline_clients_last_seen__v1 = bigquery_etl_query(
        task_id="mozilla_lockbox_derived__baseline_clients_last_seen__v1",
        destination_table="baseline_clients_last_seen_v1",
        dataset_id="mozilla_lockbox_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=True,
        task_group=task_group_lockwise_android,
    )

    with TaskGroup(
        "mozilla_lockbox_derived__baseline_clients_last_seen__v1_external",
        parent_group=task_group_lockwise_android,
    ) as mozilla_lockbox_derived__baseline_clients_last_seen__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_nondesktop__wait_for_mozilla_lockbox_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_nondesktop",
            external_task_id="wait_for_mozilla_lockbox_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=82800)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_gud__wait_for_mozilla_lockbox_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_gud",
            external_task_id="wait_for_mozilla_lockbox_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=82800)).isoformat() }}",
        )

        mozilla_lockbox_derived__baseline_clients_last_seen__v1_external.set_upstream(
            mozilla_lockbox_derived__baseline_clients_last_seen__v1
        )

    mozilla_mach_derived__baseline_clients_daily__v1 = bigquery_etl_query(
        task_id="mozilla_mach_derived__baseline_clients_daily__v1",
        destination_table="baseline_clients_daily_v1",
        dataset_id="mozilla_mach_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_mach,
    )

    mozilla_mach_derived__baseline_clients_first_seen__v1 = bigquery_etl_query(
        task_id="mozilla_mach_derived__baseline_clients_first_seen__v1",
        destination_table="baseline_clients_first_seen_v1",
        dataset_id="mozilla_mach_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=True,
        parameters=["submission_date:DATE:{{ds}}"],
        task_group=task_group_mach,
    )

    mozilla_mach_derived__baseline_clients_last_seen__v1 = bigquery_etl_query(
        task_id="mozilla_mach_derived__baseline_clients_last_seen__v1",
        destination_table="baseline_clients_last_seen_v1",
        dataset_id="mozilla_mach_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=True,
        task_group=task_group_mach,
    )

    mozilla_vpn_derived__metrics_clients_daily__v1 = bigquery_etl_query(
        task_id="mozilla_vpn_derived__metrics_clients_daily__v1",
        destination_table="metrics_clients_daily_v1",
        dataset_id="mozilla_vpn_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_mozilla_vpn,
    )

    mozillavpn_cirrus_derived__baseline_clients_daily__v1 = bigquery_etl_query(
        task_id="mozillavpn_cirrus_derived__baseline_clients_daily__v1",
        destination_table="baseline_clients_daily_v1",
        dataset_id="mozillavpn_cirrus_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_mozillavpn_cirrus,
    )

    mozillavpn_cirrus_derived__baseline_clients_first_seen__v1 = bigquery_etl_query(
        task_id="mozillavpn_cirrus_derived__baseline_clients_first_seen__v1",
        destination_table="baseline_clients_first_seen_v1",
        dataset_id="mozillavpn_cirrus_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=True,
        parameters=["submission_date:DATE:{{ds}}"],
        task_group=task_group_mozillavpn_cirrus,
    )

    mozillavpn_cirrus_derived__baseline_clients_last_seen__v1 = bigquery_etl_query(
        task_id="mozillavpn_cirrus_derived__baseline_clients_last_seen__v1",
        destination_table="baseline_clients_last_seen_v1",
        dataset_id="mozillavpn_cirrus_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=True,
        task_group=task_group_mozillavpn_cirrus,
    )

    mozillavpn_cirrus_derived__metrics_clients_daily__v1 = bigquery_etl_query(
        task_id="mozillavpn_cirrus_derived__metrics_clients_daily__v1",
        destination_table="metrics_clients_daily_v1",
        dataset_id="mozillavpn_cirrus_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_mozillavpn_cirrus,
    )

    mozillavpn_derived__baseline_clients_daily__v1 = bigquery_etl_query(
        task_id="mozillavpn_derived__baseline_clients_daily__v1",
        destination_table="baseline_clients_daily_v1",
        dataset_id="mozillavpn_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_mozilla_vpn,
    )

    mozillavpn_derived__baseline_clients_first_seen__v1 = bigquery_etl_query(
        task_id="mozillavpn_derived__baseline_clients_first_seen__v1",
        destination_table="baseline_clients_first_seen_v1",
        dataset_id="mozillavpn_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=True,
        parameters=["submission_date:DATE:{{ds}}"],
        task_group=task_group_mozilla_vpn,
    )

    mozillavpn_derived__baseline_clients_last_seen__v1 = bigquery_etl_query(
        task_id="mozillavpn_derived__baseline_clients_last_seen__v1",
        destination_table="baseline_clients_last_seen_v1",
        dataset_id="mozillavpn_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=True,
        task_group=task_group_mozilla_vpn,
    )

    mozphab_derived__baseline_clients_daily__v1 = bigquery_etl_query(
        task_id="mozphab_derived__baseline_clients_daily__v1",
        destination_table="baseline_clients_daily_v1",
        dataset_id="mozphab_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_mozphab,
    )

    mozphab_derived__baseline_clients_first_seen__v1 = bigquery_etl_query(
        task_id="mozphab_derived__baseline_clients_first_seen__v1",
        destination_table="baseline_clients_first_seen_v1",
        dataset_id="mozphab_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=True,
        parameters=["submission_date:DATE:{{ds}}"],
        task_group=task_group_mozphab,
    )

    mozphab_derived__baseline_clients_last_seen__v1 = bigquery_etl_query(
        task_id="mozphab_derived__baseline_clients_last_seen__v1",
        destination_table="baseline_clients_last_seen_v1",
        dataset_id="mozphab_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=True,
        task_group=task_group_mozphab,
    )

    mozphab_derived__clients_last_seen_joined__v1 = bigquery_etl_query(
        task_id="mozphab_derived__clients_last_seen_joined__v1",
        destination_table="clients_last_seen_joined_v1",
        dataset_id="mozphab_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=True,
        task_group=task_group_mozphab,
    )

    mozphab_derived__metrics_clients_daily__v1 = bigquery_etl_query(
        task_id="mozphab_derived__metrics_clients_daily__v1",
        destination_table="metrics_clients_daily_v1",
        dataset_id="mozphab_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_mozphab,
    )

    mozphab_derived__metrics_clients_last_seen__v1 = bigquery_etl_query(
        task_id="mozphab_derived__metrics_clients_last_seen__v1",
        destination_table="metrics_clients_last_seen_v1",
        dataset_id="mozphab_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_mozphab,
    )

    mozregression_derived__clients_last_seen_joined__v1 = bigquery_etl_query(
        task_id="mozregression_derived__clients_last_seen_joined__v1",
        destination_table="clients_last_seen_joined_v1",
        dataset_id="mozregression_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=True,
        task_group=task_group_mozregression,
    )

    mozregression_derived__metrics_clients_daily__v1 = bigquery_etl_query(
        task_id="mozregression_derived__metrics_clients_daily__v1",
        destination_table="metrics_clients_daily_v1",
        dataset_id="mozregression_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_mozregression,
    )

    mozregression_derived__metrics_clients_last_seen__v1 = bigquery_etl_query(
        task_id="mozregression_derived__metrics_clients_last_seen__v1",
        destination_table="metrics_clients_last_seen_v1",
        dataset_id="mozregression_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_mozregression,
    )

    org_mozilla_connect_firefox_derived__baseline_clients_daily__v1 = (
        bigquery_etl_query(
            task_id="org_mozilla_connect_firefox_derived__baseline_clients_daily__v1",
            destination_table="baseline_clients_daily_v1",
            dataset_id="org_mozilla_connect_firefox_derived",
            project_id="moz-fx-data-shared-prod",
            owner="ascholtz@mozilla.com",
            email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
            date_partition_parameter="submission_date",
            depends_on_past=False,
            task_group=task_group_firefox_echo_show,
        )
    )

    org_mozilla_connect_firefox_derived__baseline_clients_first_seen__v1 = bigquery_etl_query(
        task_id="org_mozilla_connect_firefox_derived__baseline_clients_first_seen__v1",
        destination_table="baseline_clients_first_seen_v1",
        dataset_id="org_mozilla_connect_firefox_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=True,
        parameters=["submission_date:DATE:{{ds}}"],
        task_group=task_group_firefox_echo_show,
    )

    org_mozilla_connect_firefox_derived__baseline_clients_last_seen__v1 = bigquery_etl_query(
        task_id="org_mozilla_connect_firefox_derived__baseline_clients_last_seen__v1",
        destination_table="baseline_clients_last_seen_v1",
        dataset_id="org_mozilla_connect_firefox_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=True,
        task_group=task_group_firefox_echo_show,
    )

    org_mozilla_fenix_derived__baseline_clients_daily__v1 = bigquery_etl_query(
        task_id="org_mozilla_fenix_derived__baseline_clients_daily__v1",
        destination_table="baseline_clients_daily_v1",
        dataset_id="org_mozilla_fenix_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_fenix,
    )

    with TaskGroup(
        "org_mozilla_fenix_derived__baseline_clients_daily__v1_external",
        parent_group=task_group_fenix,
    ) as org_mozilla_fenix_derived__baseline_clients_daily__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_org_mozilla_firefox_derived__wait_for_org_mozilla_fenix_derived__baseline_clients_daily__v1",
            external_dag_id="bqetl_org_mozilla_firefox_derived",
            external_task_id="wait_for_org_mozilla_fenix_derived__baseline_clients_daily__v1",
        )

        ExternalTaskMarker(
            task_id="bqetl_analytics_tables__wait_for_org_mozilla_fenix_derived__baseline_clients_daily__v1",
            external_dag_id="bqetl_analytics_tables",
            external_task_id="wait_for_org_mozilla_fenix_derived__baseline_clients_daily__v1",
        )

        org_mozilla_fenix_derived__baseline_clients_daily__v1_external.set_upstream(
            org_mozilla_fenix_derived__baseline_clients_daily__v1
        )

    org_mozilla_fenix_derived__baseline_clients_first_seen__v1 = bigquery_etl_query(
        task_id="org_mozilla_fenix_derived__baseline_clients_first_seen__v1",
        destination_table="baseline_clients_first_seen_v1",
        dataset_id="org_mozilla_fenix_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=True,
        parameters=["submission_date:DATE:{{ds}}"],
        task_group=task_group_fenix,
    )

    org_mozilla_fenix_derived__baseline_clients_last_seen__v1 = bigquery_etl_query(
        task_id="org_mozilla_fenix_derived__baseline_clients_last_seen__v1",
        destination_table="baseline_clients_last_seen_v1",
        dataset_id="org_mozilla_fenix_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=True,
        task_group=task_group_fenix,
    )

    with TaskGroup(
        "org_mozilla_fenix_derived__baseline_clients_last_seen__v1_external",
        parent_group=task_group_fenix,
    ) as org_mozilla_fenix_derived__baseline_clients_last_seen__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_analytics_tables__wait_for_org_mozilla_fenix_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_analytics_tables",
            external_task_id="wait_for_org_mozilla_fenix_derived__baseline_clients_last_seen__v1",
        )

        ExternalTaskMarker(
            task_id="bqetl_nondesktop__wait_for_org_mozilla_fenix_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_nondesktop",
            external_task_id="wait_for_org_mozilla_fenix_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=82800)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_gud__wait_for_org_mozilla_fenix_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_gud",
            external_task_id="wait_for_org_mozilla_fenix_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=82800)).isoformat() }}",
        )

        org_mozilla_fenix_derived__baseline_clients_last_seen__v1_external.set_upstream(
            org_mozilla_fenix_derived__baseline_clients_last_seen__v1
        )

    org_mozilla_fenix_nightly_derived__baseline_clients_daily__v1 = bigquery_etl_query(
        task_id="org_mozilla_fenix_nightly_derived__baseline_clients_daily__v1",
        destination_table="baseline_clients_daily_v1",
        dataset_id="org_mozilla_fenix_nightly_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_fenix,
    )

    with TaskGroup(
        "org_mozilla_fenix_nightly_derived__baseline_clients_daily__v1_external",
        parent_group=task_group_fenix,
    ) as org_mozilla_fenix_nightly_derived__baseline_clients_daily__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_org_mozilla_firefox_derived__wait_for_org_mozilla_fenix_nightly_derived__baseline_clients_daily__v1",
            external_dag_id="bqetl_org_mozilla_firefox_derived",
            external_task_id="wait_for_org_mozilla_fenix_nightly_derived__baseline_clients_daily__v1",
        )

        ExternalTaskMarker(
            task_id="bqetl_analytics_tables__wait_for_org_mozilla_fenix_nightly_derived__baseline_clients_daily__v1",
            external_dag_id="bqetl_analytics_tables",
            external_task_id="wait_for_org_mozilla_fenix_nightly_derived__baseline_clients_daily__v1",
        )

        org_mozilla_fenix_nightly_derived__baseline_clients_daily__v1_external.set_upstream(
            org_mozilla_fenix_nightly_derived__baseline_clients_daily__v1
        )

    org_mozilla_fenix_nightly_derived__baseline_clients_first_seen__v1 = bigquery_etl_query(
        task_id="org_mozilla_fenix_nightly_derived__baseline_clients_first_seen__v1",
        destination_table="baseline_clients_first_seen_v1",
        dataset_id="org_mozilla_fenix_nightly_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=True,
        parameters=["submission_date:DATE:{{ds}}"],
        task_group=task_group_fenix,
    )

    org_mozilla_fenix_nightly_derived__baseline_clients_last_seen__v1 = (
        bigquery_etl_query(
            task_id="org_mozilla_fenix_nightly_derived__baseline_clients_last_seen__v1",
            destination_table="baseline_clients_last_seen_v1",
            dataset_id="org_mozilla_fenix_nightly_derived",
            project_id="moz-fx-data-shared-prod",
            owner="ascholtz@mozilla.com",
            email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
            date_partition_parameter="submission_date",
            depends_on_past=True,
            task_group=task_group_fenix,
        )
    )

    with TaskGroup(
        "org_mozilla_fenix_nightly_derived__baseline_clients_last_seen__v1_external",
        parent_group=task_group_fenix,
    ) as org_mozilla_fenix_nightly_derived__baseline_clients_last_seen__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_analytics_tables__wait_for_org_mozilla_fenix_nightly_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_analytics_tables",
            external_task_id="wait_for_org_mozilla_fenix_nightly_derived__baseline_clients_last_seen__v1",
        )

        ExternalTaskMarker(
            task_id="bqetl_nondesktop__wait_for_org_mozilla_fenix_nightly_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_nondesktop",
            external_task_id="wait_for_org_mozilla_fenix_nightly_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=82800)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_gud__wait_for_org_mozilla_fenix_nightly_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_gud",
            external_task_id="wait_for_org_mozilla_fenix_nightly_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=82800)).isoformat() }}",
        )

        org_mozilla_fenix_nightly_derived__baseline_clients_last_seen__v1_external.set_upstream(
            org_mozilla_fenix_nightly_derived__baseline_clients_last_seen__v1
        )

    org_mozilla_fennec_aurora_derived__baseline_clients_daily__v1 = bigquery_etl_query(
        task_id="org_mozilla_fennec_aurora_derived__baseline_clients_daily__v1",
        destination_table="baseline_clients_daily_v1",
        dataset_id="org_mozilla_fennec_aurora_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_fenix,
    )

    with TaskGroup(
        "org_mozilla_fennec_aurora_derived__baseline_clients_daily__v1_external",
        parent_group=task_group_fenix,
    ) as org_mozilla_fennec_aurora_derived__baseline_clients_daily__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_org_mozilla_firefox_derived__wait_for_org_mozilla_fennec_aurora_derived__baseline_clients_daily__v1",
            external_dag_id="bqetl_org_mozilla_firefox_derived",
            external_task_id="wait_for_org_mozilla_fennec_aurora_derived__baseline_clients_daily__v1",
        )

        ExternalTaskMarker(
            task_id="bqetl_analytics_tables__wait_for_org_mozilla_fennec_aurora_derived__baseline_clients_daily__v1",
            external_dag_id="bqetl_analytics_tables",
            external_task_id="wait_for_org_mozilla_fennec_aurora_derived__baseline_clients_daily__v1",
        )

        org_mozilla_fennec_aurora_derived__baseline_clients_daily__v1_external.set_upstream(
            org_mozilla_fennec_aurora_derived__baseline_clients_daily__v1
        )

    org_mozilla_fennec_aurora_derived__baseline_clients_first_seen__v1 = bigquery_etl_query(
        task_id="org_mozilla_fennec_aurora_derived__baseline_clients_first_seen__v1",
        destination_table="baseline_clients_first_seen_v1",
        dataset_id="org_mozilla_fennec_aurora_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=True,
        parameters=["submission_date:DATE:{{ds}}"],
        task_group=task_group_fenix,
    )

    org_mozilla_fennec_aurora_derived__baseline_clients_last_seen__v1 = (
        bigquery_etl_query(
            task_id="org_mozilla_fennec_aurora_derived__baseline_clients_last_seen__v1",
            destination_table="baseline_clients_last_seen_v1",
            dataset_id="org_mozilla_fennec_aurora_derived",
            project_id="moz-fx-data-shared-prod",
            owner="ascholtz@mozilla.com",
            email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
            date_partition_parameter="submission_date",
            depends_on_past=True,
            task_group=task_group_fenix,
        )
    )

    with TaskGroup(
        "org_mozilla_fennec_aurora_derived__baseline_clients_last_seen__v1_external",
        parent_group=task_group_fenix,
    ) as org_mozilla_fennec_aurora_derived__baseline_clients_last_seen__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_analytics_tables__wait_for_org_mozilla_fennec_aurora_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_analytics_tables",
            external_task_id="wait_for_org_mozilla_fennec_aurora_derived__baseline_clients_last_seen__v1",
        )

        ExternalTaskMarker(
            task_id="bqetl_nondesktop__wait_for_org_mozilla_fennec_aurora_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_nondesktop",
            external_task_id="wait_for_org_mozilla_fennec_aurora_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=82800)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_gud__wait_for_org_mozilla_fennec_aurora_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_gud",
            external_task_id="wait_for_org_mozilla_fennec_aurora_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=82800)).isoformat() }}",
        )

        org_mozilla_fennec_aurora_derived__baseline_clients_last_seen__v1_external.set_upstream(
            org_mozilla_fennec_aurora_derived__baseline_clients_last_seen__v1
        )

    org_mozilla_firefox_beta_derived__baseline_clients_daily__v1 = bigquery_etl_query(
        task_id="org_mozilla_firefox_beta_derived__baseline_clients_daily__v1",
        destination_table="baseline_clients_daily_v1",
        dataset_id="org_mozilla_firefox_beta_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_fenix,
    )

    with TaskGroup(
        "org_mozilla_firefox_beta_derived__baseline_clients_daily__v1_external",
        parent_group=task_group_fenix,
    ) as org_mozilla_firefox_beta_derived__baseline_clients_daily__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_org_mozilla_firefox_derived__wait_for_org_mozilla_firefox_beta_derived__baseline_clients_daily__v1",
            external_dag_id="bqetl_org_mozilla_firefox_derived",
            external_task_id="wait_for_org_mozilla_firefox_beta_derived__baseline_clients_daily__v1",
        )

        ExternalTaskMarker(
            task_id="bqetl_analytics_tables__wait_for_org_mozilla_firefox_beta_derived__baseline_clients_daily__v1",
            external_dag_id="bqetl_analytics_tables",
            external_task_id="wait_for_org_mozilla_firefox_beta_derived__baseline_clients_daily__v1",
        )

        org_mozilla_firefox_beta_derived__baseline_clients_daily__v1_external.set_upstream(
            org_mozilla_firefox_beta_derived__baseline_clients_daily__v1
        )

    org_mozilla_firefox_beta_derived__baseline_clients_first_seen__v1 = (
        bigquery_etl_query(
            task_id="org_mozilla_firefox_beta_derived__baseline_clients_first_seen__v1",
            destination_table="baseline_clients_first_seen_v1",
            dataset_id="org_mozilla_firefox_beta_derived",
            project_id="moz-fx-data-shared-prod",
            owner="ascholtz@mozilla.com",
            email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
            date_partition_parameter=None,
            depends_on_past=True,
            parameters=["submission_date:DATE:{{ds}}"],
            task_group=task_group_fenix,
        )
    )

    org_mozilla_firefox_beta_derived__baseline_clients_last_seen__v1 = (
        bigquery_etl_query(
            task_id="org_mozilla_firefox_beta_derived__baseline_clients_last_seen__v1",
            destination_table="baseline_clients_last_seen_v1",
            dataset_id="org_mozilla_firefox_beta_derived",
            project_id="moz-fx-data-shared-prod",
            owner="ascholtz@mozilla.com",
            email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
            date_partition_parameter="submission_date",
            depends_on_past=True,
            task_group=task_group_fenix,
        )
    )

    with TaskGroup(
        "org_mozilla_firefox_beta_derived__baseline_clients_last_seen__v1_external",
        parent_group=task_group_fenix,
    ) as org_mozilla_firefox_beta_derived__baseline_clients_last_seen__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_analytics_tables__wait_for_org_mozilla_firefox_beta_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_analytics_tables",
            external_task_id="wait_for_org_mozilla_firefox_beta_derived__baseline_clients_last_seen__v1",
        )

        ExternalTaskMarker(
            task_id="bqetl_nondesktop__wait_for_org_mozilla_firefox_beta_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_nondesktop",
            external_task_id="wait_for_org_mozilla_firefox_beta_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=82800)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_gud__wait_for_org_mozilla_firefox_beta_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_gud",
            external_task_id="wait_for_org_mozilla_firefox_beta_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=82800)).isoformat() }}",
        )

        org_mozilla_firefox_beta_derived__baseline_clients_last_seen__v1_external.set_upstream(
            org_mozilla_firefox_beta_derived__baseline_clients_last_seen__v1
        )

    org_mozilla_firefox_derived__baseline_clients_daily__v1 = bigquery_etl_query(
        task_id="org_mozilla_firefox_derived__baseline_clients_daily__v1",
        destination_table="baseline_clients_daily_v1",
        dataset_id="org_mozilla_firefox_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_fenix,
    )

    with TaskGroup(
        "org_mozilla_firefox_derived__baseline_clients_daily__v1_external",
        parent_group=task_group_fenix,
    ) as org_mozilla_firefox_derived__baseline_clients_daily__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_org_mozilla_firefox_derived__wait_for_org_mozilla_firefox_derived__baseline_clients_daily__v1",
            external_dag_id="bqetl_org_mozilla_firefox_derived",
            external_task_id="wait_for_org_mozilla_firefox_derived__baseline_clients_daily__v1",
        )

        ExternalTaskMarker(
            task_id="bqetl_analytics_tables__wait_for_org_mozilla_firefox_derived__baseline_clients_daily__v1",
            external_dag_id="bqetl_analytics_tables",
            external_task_id="wait_for_org_mozilla_firefox_derived__baseline_clients_daily__v1",
        )

        org_mozilla_firefox_derived__baseline_clients_daily__v1_external.set_upstream(
            org_mozilla_firefox_derived__baseline_clients_daily__v1
        )

    org_mozilla_firefox_derived__baseline_clients_first_seen__v1 = bigquery_etl_query(
        task_id="org_mozilla_firefox_derived__baseline_clients_first_seen__v1",
        destination_table="baseline_clients_first_seen_v1",
        dataset_id="org_mozilla_firefox_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=True,
        parameters=["submission_date:DATE:{{ds}}"],
        task_group=task_group_fenix,
    )

    org_mozilla_firefox_derived__baseline_clients_last_seen__v1 = bigquery_etl_query(
        task_id="org_mozilla_firefox_derived__baseline_clients_last_seen__v1",
        destination_table="baseline_clients_last_seen_v1",
        dataset_id="org_mozilla_firefox_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=True,
        task_group=task_group_fenix,
    )

    with TaskGroup(
        "org_mozilla_firefox_derived__baseline_clients_last_seen__v1_external",
        parent_group=task_group_fenix,
    ) as org_mozilla_firefox_derived__baseline_clients_last_seen__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_analytics_tables__wait_for_org_mozilla_firefox_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_analytics_tables",
            external_task_id="wait_for_org_mozilla_firefox_derived__baseline_clients_last_seen__v1",
        )

        ExternalTaskMarker(
            task_id="bqetl_nondesktop__wait_for_org_mozilla_firefox_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_nondesktop",
            external_task_id="wait_for_org_mozilla_firefox_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=82800)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_gud__wait_for_org_mozilla_firefox_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_gud",
            external_task_id="wait_for_org_mozilla_firefox_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=82800)).isoformat() }}",
        )

        org_mozilla_firefox_derived__baseline_clients_last_seen__v1_external.set_upstream(
            org_mozilla_firefox_derived__baseline_clients_last_seen__v1
        )

    org_mozilla_firefox_vpn_derived__baseline_clients_daily__v1 = bigquery_etl_query(
        task_id="org_mozilla_firefox_vpn_derived__baseline_clients_daily__v1",
        destination_table="baseline_clients_daily_v1",
        dataset_id="org_mozilla_firefox_vpn_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_mozilla_vpn,
    )

    org_mozilla_firefox_vpn_derived__baseline_clients_first_seen__v1 = (
        bigquery_etl_query(
            task_id="org_mozilla_firefox_vpn_derived__baseline_clients_first_seen__v1",
            destination_table="baseline_clients_first_seen_v1",
            dataset_id="org_mozilla_firefox_vpn_derived",
            project_id="moz-fx-data-shared-prod",
            owner="ascholtz@mozilla.com",
            email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
            date_partition_parameter=None,
            depends_on_past=True,
            parameters=["submission_date:DATE:{{ds}}"],
            task_group=task_group_mozilla_vpn,
        )
    )

    org_mozilla_firefox_vpn_derived__baseline_clients_last_seen__v1 = (
        bigquery_etl_query(
            task_id="org_mozilla_firefox_vpn_derived__baseline_clients_last_seen__v1",
            destination_table="baseline_clients_last_seen_v1",
            dataset_id="org_mozilla_firefox_vpn_derived",
            project_id="moz-fx-data-shared-prod",
            owner="ascholtz@mozilla.com",
            email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
            date_partition_parameter="submission_date",
            depends_on_past=True,
            task_group=task_group_mozilla_vpn,
        )
    )

    org_mozilla_firefoxreality_derived__baseline_clients_daily__v1 = bigquery_etl_query(
        task_id="org_mozilla_firefoxreality_derived__baseline_clients_daily__v1",
        destination_table="baseline_clients_daily_v1",
        dataset_id="org_mozilla_firefoxreality_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_firefox_reality_pc,
    )

    org_mozilla_firefoxreality_derived__baseline_clients_first_seen__v1 = bigquery_etl_query(
        task_id="org_mozilla_firefoxreality_derived__baseline_clients_first_seen__v1",
        destination_table="baseline_clients_first_seen_v1",
        dataset_id="org_mozilla_firefoxreality_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=True,
        parameters=["submission_date:DATE:{{ds}}"],
        task_group=task_group_firefox_reality_pc,
    )

    org_mozilla_firefoxreality_derived__baseline_clients_last_seen__v1 = bigquery_etl_query(
        task_id="org_mozilla_firefoxreality_derived__baseline_clients_last_seen__v1",
        destination_table="baseline_clients_last_seen_v1",
        dataset_id="org_mozilla_firefoxreality_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=True,
        task_group=task_group_firefox_reality_pc,
    )

    org_mozilla_focus_beta_derived__baseline_clients_daily__v1 = bigquery_etl_query(
        task_id="org_mozilla_focus_beta_derived__baseline_clients_daily__v1",
        destination_table="baseline_clients_daily_v1",
        dataset_id="org_mozilla_focus_beta_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_focus_android,
    )

    org_mozilla_focus_beta_derived__baseline_clients_first_seen__v1 = (
        bigquery_etl_query(
            task_id="org_mozilla_focus_beta_derived__baseline_clients_first_seen__v1",
            destination_table="baseline_clients_first_seen_v1",
            dataset_id="org_mozilla_focus_beta_derived",
            project_id="moz-fx-data-shared-prod",
            owner="ascholtz@mozilla.com",
            email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
            date_partition_parameter=None,
            depends_on_past=True,
            parameters=["submission_date:DATE:{{ds}}"],
            task_group=task_group_focus_android,
        )
    )

    org_mozilla_focus_beta_derived__baseline_clients_last_seen__v1 = bigquery_etl_query(
        task_id="org_mozilla_focus_beta_derived__baseline_clients_last_seen__v1",
        destination_table="baseline_clients_last_seen_v1",
        dataset_id="org_mozilla_focus_beta_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=True,
        task_group=task_group_focus_android,
    )

    org_mozilla_focus_derived__baseline_clients_daily__v1 = bigquery_etl_query(
        task_id="org_mozilla_focus_derived__baseline_clients_daily__v1",
        destination_table="baseline_clients_daily_v1",
        dataset_id="org_mozilla_focus_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_focus_android,
    )

    org_mozilla_focus_derived__baseline_clients_first_seen__v1 = bigquery_etl_query(
        task_id="org_mozilla_focus_derived__baseline_clients_first_seen__v1",
        destination_table="baseline_clients_first_seen_v1",
        dataset_id="org_mozilla_focus_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=True,
        parameters=["submission_date:DATE:{{ds}}"],
        task_group=task_group_focus_android,
    )

    org_mozilla_focus_derived__baseline_clients_last_seen__v1 = bigquery_etl_query(
        task_id="org_mozilla_focus_derived__baseline_clients_last_seen__v1",
        destination_table="baseline_clients_last_seen_v1",
        dataset_id="org_mozilla_focus_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=True,
        task_group=task_group_focus_android,
    )

    with TaskGroup(
        "org_mozilla_focus_derived__baseline_clients_last_seen__v1_external",
        parent_group=task_group_focus_android,
    ) as org_mozilla_focus_derived__baseline_clients_last_seen__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_nondesktop__wait_for_org_mozilla_focus_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_nondesktop",
            external_task_id="wait_for_org_mozilla_focus_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=82800)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_gud__wait_for_org_mozilla_focus_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_gud",
            external_task_id="wait_for_org_mozilla_focus_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=82800)).isoformat() }}",
        )

        org_mozilla_focus_derived__baseline_clients_last_seen__v1_external.set_upstream(
            org_mozilla_focus_derived__baseline_clients_last_seen__v1
        )

    org_mozilla_focus_nightly_derived__baseline_clients_daily__v1 = bigquery_etl_query(
        task_id="org_mozilla_focus_nightly_derived__baseline_clients_daily__v1",
        destination_table="baseline_clients_daily_v1",
        dataset_id="org_mozilla_focus_nightly_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_focus_android,
    )

    org_mozilla_focus_nightly_derived__baseline_clients_first_seen__v1 = bigquery_etl_query(
        task_id="org_mozilla_focus_nightly_derived__baseline_clients_first_seen__v1",
        destination_table="baseline_clients_first_seen_v1",
        dataset_id="org_mozilla_focus_nightly_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=True,
        parameters=["submission_date:DATE:{{ds}}"],
        task_group=task_group_focus_android,
    )

    org_mozilla_focus_nightly_derived__baseline_clients_last_seen__v1 = (
        bigquery_etl_query(
            task_id="org_mozilla_focus_nightly_derived__baseline_clients_last_seen__v1",
            destination_table="baseline_clients_last_seen_v1",
            dataset_id="org_mozilla_focus_nightly_derived",
            project_id="moz-fx-data-shared-prod",
            owner="ascholtz@mozilla.com",
            email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
            date_partition_parameter="submission_date",
            depends_on_past=True,
            task_group=task_group_focus_android,
        )
    )

    org_mozilla_ios_fennec_derived__baseline_clients_daily__v1 = bigquery_etl_query(
        task_id="org_mozilla_ios_fennec_derived__baseline_clients_daily__v1",
        destination_table="baseline_clients_daily_v1",
        dataset_id="org_mozilla_ios_fennec_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_firefox_ios,
    )

    with TaskGroup(
        "org_mozilla_ios_fennec_derived__baseline_clients_daily__v1_external",
        parent_group=task_group_firefox_ios,
    ) as org_mozilla_ios_fennec_derived__baseline_clients_daily__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_firefox_ios__wait_for_org_mozilla_ios_fennec_derived__baseline_clients_daily__v1",
            external_dag_id="bqetl_firefox_ios",
            external_task_id="wait_for_org_mozilla_ios_fennec_derived__baseline_clients_daily__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=79200)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_mobile_activation__wait_for_org_mozilla_ios_fennec_derived__baseline_clients_daily__v1",
            external_dag_id="bqetl_mobile_activation",
            external_task_id="wait_for_org_mozilla_ios_fennec_derived__baseline_clients_daily__v1",
            execution_date="{{ (execution_date - macros.timedelta(seconds=7200)).isoformat() }}",
        )

        org_mozilla_ios_fennec_derived__baseline_clients_daily__v1_external.set_upstream(
            org_mozilla_ios_fennec_derived__baseline_clients_daily__v1
        )

    org_mozilla_ios_fennec_derived__baseline_clients_first_seen__v1 = (
        bigquery_etl_query(
            task_id="org_mozilla_ios_fennec_derived__baseline_clients_first_seen__v1",
            destination_table="baseline_clients_first_seen_v1",
            dataset_id="org_mozilla_ios_fennec_derived",
            project_id="moz-fx-data-shared-prod",
            owner="ascholtz@mozilla.com",
            email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
            date_partition_parameter=None,
            depends_on_past=True,
            parameters=["submission_date:DATE:{{ds}}"],
            task_group=task_group_firefox_ios,
        )
    )

    org_mozilla_ios_fennec_derived__baseline_clients_last_seen__v1 = bigquery_etl_query(
        task_id="org_mozilla_ios_fennec_derived__baseline_clients_last_seen__v1",
        destination_table="baseline_clients_last_seen_v1",
        dataset_id="org_mozilla_ios_fennec_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=True,
        task_group=task_group_firefox_ios,
    )

    with TaskGroup(
        "org_mozilla_ios_fennec_derived__baseline_clients_last_seen__v1_external",
        parent_group=task_group_firefox_ios,
    ) as org_mozilla_ios_fennec_derived__baseline_clients_last_seen__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_firefox_ios__wait_for_org_mozilla_ios_fennec_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_firefox_ios",
            external_task_id="wait_for_org_mozilla_ios_fennec_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=79200)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_mobile_activation__wait_for_org_mozilla_ios_fennec_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_mobile_activation",
            external_task_id="wait_for_org_mozilla_ios_fennec_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(seconds=7200)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_nondesktop__wait_for_org_mozilla_ios_fennec_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_nondesktop",
            external_task_id="wait_for_org_mozilla_ios_fennec_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=82800)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_gud__wait_for_org_mozilla_ios_fennec_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_gud",
            external_task_id="wait_for_org_mozilla_ios_fennec_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=82800)).isoformat() }}",
        )

        org_mozilla_ios_fennec_derived__baseline_clients_last_seen__v1_external.set_upstream(
            org_mozilla_ios_fennec_derived__baseline_clients_last_seen__v1
        )

    org_mozilla_ios_firefox_derived__baseline_clients_daily__v1 = bigquery_etl_query(
        task_id="org_mozilla_ios_firefox_derived__baseline_clients_daily__v1",
        destination_table="baseline_clients_daily_v1",
        dataset_id="org_mozilla_ios_firefox_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_firefox_ios,
    )

    with TaskGroup(
        "org_mozilla_ios_firefox_derived__baseline_clients_daily__v1_external",
        parent_group=task_group_firefox_ios,
    ) as org_mozilla_ios_firefox_derived__baseline_clients_daily__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_firefox_ios__wait_for_org_mozilla_ios_firefox_derived__baseline_clients_daily__v1",
            external_dag_id="bqetl_firefox_ios",
            external_task_id="wait_for_org_mozilla_ios_firefox_derived__baseline_clients_daily__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=79200)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_mobile_activation__wait_for_org_mozilla_ios_firefox_derived__baseline_clients_daily__v1",
            external_dag_id="bqetl_mobile_activation",
            external_task_id="wait_for_org_mozilla_ios_firefox_derived__baseline_clients_daily__v1",
            execution_date="{{ (execution_date - macros.timedelta(seconds=7200)).isoformat() }}",
        )

        org_mozilla_ios_firefox_derived__baseline_clients_daily__v1_external.set_upstream(
            org_mozilla_ios_firefox_derived__baseline_clients_daily__v1
        )

    org_mozilla_ios_firefox_derived__baseline_clients_first_seen__v1 = (
        bigquery_etl_query(
            task_id="org_mozilla_ios_firefox_derived__baseline_clients_first_seen__v1",
            destination_table="baseline_clients_first_seen_v1",
            dataset_id="org_mozilla_ios_firefox_derived",
            project_id="moz-fx-data-shared-prod",
            owner="ascholtz@mozilla.com",
            email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
            date_partition_parameter=None,
            depends_on_past=True,
            parameters=["submission_date:DATE:{{ds}}"],
            task_group=task_group_firefox_ios,
        )
    )

    org_mozilla_ios_firefox_derived__baseline_clients_last_seen__v1 = (
        bigquery_etl_query(
            task_id="org_mozilla_ios_firefox_derived__baseline_clients_last_seen__v1",
            destination_table="baseline_clients_last_seen_v1",
            dataset_id="org_mozilla_ios_firefox_derived",
            project_id="moz-fx-data-shared-prod",
            owner="ascholtz@mozilla.com",
            email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
            date_partition_parameter="submission_date",
            depends_on_past=True,
            task_group=task_group_firefox_ios,
        )
    )

    with TaskGroup(
        "org_mozilla_ios_firefox_derived__baseline_clients_last_seen__v1_external",
        parent_group=task_group_firefox_ios,
    ) as org_mozilla_ios_firefox_derived__baseline_clients_last_seen__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_firefox_ios__wait_for_org_mozilla_ios_firefox_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_firefox_ios",
            external_task_id="wait_for_org_mozilla_ios_firefox_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=79200)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_mobile_activation__wait_for_org_mozilla_ios_firefox_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_mobile_activation",
            external_task_id="wait_for_org_mozilla_ios_firefox_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(seconds=7200)).isoformat() }}",
        )

        org_mozilla_ios_firefox_derived__baseline_clients_last_seen__v1_external.set_upstream(
            org_mozilla_ios_firefox_derived__baseline_clients_last_seen__v1
        )

    org_mozilla_ios_firefoxbeta_derived__baseline_clients_daily__v1 = (
        bigquery_etl_query(
            task_id="org_mozilla_ios_firefoxbeta_derived__baseline_clients_daily__v1",
            destination_table="baseline_clients_daily_v1",
            dataset_id="org_mozilla_ios_firefoxbeta_derived",
            project_id="moz-fx-data-shared-prod",
            owner="ascholtz@mozilla.com",
            email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
            date_partition_parameter="submission_date",
            depends_on_past=False,
            task_group=task_group_firefox_ios,
        )
    )

    with TaskGroup(
        "org_mozilla_ios_firefoxbeta_derived__baseline_clients_daily__v1_external",
        parent_group=task_group_firefox_ios,
    ) as org_mozilla_ios_firefoxbeta_derived__baseline_clients_daily__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_firefox_ios__wait_for_org_mozilla_ios_firefoxbeta_derived__baseline_clients_daily__v1",
            external_dag_id="bqetl_firefox_ios",
            external_task_id="wait_for_org_mozilla_ios_firefoxbeta_derived__baseline_clients_daily__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=79200)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_mobile_activation__wait_for_org_mozilla_ios_firefoxbeta_derived__baseline_clients_daily__v1",
            external_dag_id="bqetl_mobile_activation",
            external_task_id="wait_for_org_mozilla_ios_firefoxbeta_derived__baseline_clients_daily__v1",
            execution_date="{{ (execution_date - macros.timedelta(seconds=7200)).isoformat() }}",
        )

        org_mozilla_ios_firefoxbeta_derived__baseline_clients_daily__v1_external.set_upstream(
            org_mozilla_ios_firefoxbeta_derived__baseline_clients_daily__v1
        )

    org_mozilla_ios_firefoxbeta_derived__baseline_clients_first_seen__v1 = bigquery_etl_query(
        task_id="org_mozilla_ios_firefoxbeta_derived__baseline_clients_first_seen__v1",
        destination_table="baseline_clients_first_seen_v1",
        dataset_id="org_mozilla_ios_firefoxbeta_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=True,
        parameters=["submission_date:DATE:{{ds}}"],
        task_group=task_group_firefox_ios,
    )

    org_mozilla_ios_firefoxbeta_derived__baseline_clients_last_seen__v1 = bigquery_etl_query(
        task_id="org_mozilla_ios_firefoxbeta_derived__baseline_clients_last_seen__v1",
        destination_table="baseline_clients_last_seen_v1",
        dataset_id="org_mozilla_ios_firefoxbeta_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=True,
        task_group=task_group_firefox_ios,
    )

    with TaskGroup(
        "org_mozilla_ios_firefoxbeta_derived__baseline_clients_last_seen__v1_external",
        parent_group=task_group_firefox_ios,
    ) as org_mozilla_ios_firefoxbeta_derived__baseline_clients_last_seen__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_firefox_ios__wait_for_org_mozilla_ios_firefoxbeta_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_firefox_ios",
            external_task_id="wait_for_org_mozilla_ios_firefoxbeta_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=79200)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_mobile_activation__wait_for_org_mozilla_ios_firefoxbeta_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_mobile_activation",
            external_task_id="wait_for_org_mozilla_ios_firefoxbeta_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(seconds=7200)).isoformat() }}",
        )

        org_mozilla_ios_firefoxbeta_derived__baseline_clients_last_seen__v1_external.set_upstream(
            org_mozilla_ios_firefoxbeta_derived__baseline_clients_last_seen__v1
        )

    org_mozilla_ios_firefoxvpn_derived__baseline_clients_daily__v1 = bigquery_etl_query(
        task_id="org_mozilla_ios_firefoxvpn_derived__baseline_clients_daily__v1",
        destination_table="baseline_clients_daily_v1",
        dataset_id="org_mozilla_ios_firefoxvpn_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_mozilla_vpn,
    )

    org_mozilla_ios_firefoxvpn_derived__baseline_clients_first_seen__v1 = bigquery_etl_query(
        task_id="org_mozilla_ios_firefoxvpn_derived__baseline_clients_first_seen__v1",
        destination_table="baseline_clients_first_seen_v1",
        dataset_id="org_mozilla_ios_firefoxvpn_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=True,
        parameters=["submission_date:DATE:{{ds}}"],
        task_group=task_group_mozilla_vpn,
    )

    org_mozilla_ios_firefoxvpn_derived__baseline_clients_last_seen__v1 = bigquery_etl_query(
        task_id="org_mozilla_ios_firefoxvpn_derived__baseline_clients_last_seen__v1",
        destination_table="baseline_clients_last_seen_v1",
        dataset_id="org_mozilla_ios_firefoxvpn_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=True,
        task_group=task_group_mozilla_vpn,
    )

    org_mozilla_ios_firefoxvpn_network_extension_derived__baseline_clients_daily__v1 = bigquery_etl_query(
        task_id="org_mozilla_ios_firefoxvpn_network_extension_derived__baseline_clients_daily__v1",
        destination_table="baseline_clients_daily_v1",
        dataset_id="org_mozilla_ios_firefoxvpn_network_extension_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_mozilla_vpn,
    )

    org_mozilla_ios_firefoxvpn_network_extension_derived__baseline_clients_first_seen__v1 = bigquery_etl_query(
        task_id="org_mozilla_ios_firefoxvpn_network_extension_derived__baseline_clients_first_seen__v1",
        destination_table="baseline_clients_first_seen_v1",
        dataset_id="org_mozilla_ios_firefoxvpn_network_extension_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=True,
        parameters=["submission_date:DATE:{{ds}}"],
        task_group=task_group_mozilla_vpn,
    )

    org_mozilla_ios_firefoxvpn_network_extension_derived__baseline_clients_last_seen__v1 = bigquery_etl_query(
        task_id="org_mozilla_ios_firefoxvpn_network_extension_derived__baseline_clients_last_seen__v1",
        destination_table="baseline_clients_last_seen_v1",
        dataset_id="org_mozilla_ios_firefoxvpn_network_extension_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=True,
        task_group=task_group_mozilla_vpn,
    )

    org_mozilla_ios_focus_derived__baseline_clients_daily__v1 = bigquery_etl_query(
        task_id="org_mozilla_ios_focus_derived__baseline_clients_daily__v1",
        destination_table="baseline_clients_daily_v1",
        dataset_id="org_mozilla_ios_focus_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_focus_ios,
    )

    org_mozilla_ios_focus_derived__baseline_clients_first_seen__v1 = bigquery_etl_query(
        task_id="org_mozilla_ios_focus_derived__baseline_clients_first_seen__v1",
        destination_table="baseline_clients_first_seen_v1",
        dataset_id="org_mozilla_ios_focus_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=True,
        parameters=["submission_date:DATE:{{ds}}"],
        task_group=task_group_focus_ios,
    )

    org_mozilla_ios_focus_derived__baseline_clients_last_seen__v1 = bigquery_etl_query(
        task_id="org_mozilla_ios_focus_derived__baseline_clients_last_seen__v1",
        destination_table="baseline_clients_last_seen_v1",
        dataset_id="org_mozilla_ios_focus_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=True,
        task_group=task_group_focus_ios,
    )

    with TaskGroup(
        "org_mozilla_ios_focus_derived__baseline_clients_last_seen__v1_external",
        parent_group=task_group_focus_ios,
    ) as org_mozilla_ios_focus_derived__baseline_clients_last_seen__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_nondesktop__wait_for_org_mozilla_ios_focus_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_nondesktop",
            external_task_id="wait_for_org_mozilla_ios_focus_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=82800)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_gud__wait_for_org_mozilla_ios_focus_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_gud",
            external_task_id="wait_for_org_mozilla_ios_focus_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=82800)).isoformat() }}",
        )

        org_mozilla_ios_focus_derived__baseline_clients_last_seen__v1_external.set_upstream(
            org_mozilla_ios_focus_derived__baseline_clients_last_seen__v1
        )

    org_mozilla_ios_klar_derived__baseline_clients_daily__v1 = bigquery_etl_query(
        task_id="org_mozilla_ios_klar_derived__baseline_clients_daily__v1",
        destination_table="baseline_clients_daily_v1",
        dataset_id="org_mozilla_ios_klar_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_klar_ios,
    )

    org_mozilla_ios_klar_derived__baseline_clients_first_seen__v1 = bigquery_etl_query(
        task_id="org_mozilla_ios_klar_derived__baseline_clients_first_seen__v1",
        destination_table="baseline_clients_first_seen_v1",
        dataset_id="org_mozilla_ios_klar_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=True,
        parameters=["submission_date:DATE:{{ds}}"],
        task_group=task_group_klar_ios,
    )

    org_mozilla_ios_klar_derived__baseline_clients_last_seen__v1 = bigquery_etl_query(
        task_id="org_mozilla_ios_klar_derived__baseline_clients_last_seen__v1",
        destination_table="baseline_clients_last_seen_v1",
        dataset_id="org_mozilla_ios_klar_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=True,
        task_group=task_group_klar_ios,
    )

    with TaskGroup(
        "org_mozilla_ios_klar_derived__baseline_clients_last_seen__v1_external",
        parent_group=task_group_klar_ios,
    ) as org_mozilla_ios_klar_derived__baseline_clients_last_seen__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_nondesktop__wait_for_org_mozilla_ios_klar_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_nondesktop",
            external_task_id="wait_for_org_mozilla_ios_klar_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=82800)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_gud__wait_for_org_mozilla_ios_klar_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_gud",
            external_task_id="wait_for_org_mozilla_ios_klar_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=82800)).isoformat() }}",
        )

        org_mozilla_ios_klar_derived__baseline_clients_last_seen__v1_external.set_upstream(
            org_mozilla_ios_klar_derived__baseline_clients_last_seen__v1
        )

    org_mozilla_ios_lockbox_derived__baseline_clients_daily__v1 = bigquery_etl_query(
        task_id="org_mozilla_ios_lockbox_derived__baseline_clients_daily__v1",
        destination_table="baseline_clients_daily_v1",
        dataset_id="org_mozilla_ios_lockbox_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_lockwise_ios,
    )

    org_mozilla_ios_lockbox_derived__baseline_clients_first_seen__v1 = (
        bigquery_etl_query(
            task_id="org_mozilla_ios_lockbox_derived__baseline_clients_first_seen__v1",
            destination_table="baseline_clients_first_seen_v1",
            dataset_id="org_mozilla_ios_lockbox_derived",
            project_id="moz-fx-data-shared-prod",
            owner="ascholtz@mozilla.com",
            email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
            date_partition_parameter=None,
            depends_on_past=True,
            parameters=["submission_date:DATE:{{ds}}"],
            task_group=task_group_lockwise_ios,
        )
    )

    org_mozilla_ios_lockbox_derived__baseline_clients_last_seen__v1 = (
        bigquery_etl_query(
            task_id="org_mozilla_ios_lockbox_derived__baseline_clients_last_seen__v1",
            destination_table="baseline_clients_last_seen_v1",
            dataset_id="org_mozilla_ios_lockbox_derived",
            project_id="moz-fx-data-shared-prod",
            owner="ascholtz@mozilla.com",
            email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
            date_partition_parameter="submission_date",
            depends_on_past=True,
            task_group=task_group_lockwise_ios,
        )
    )

    with TaskGroup(
        "org_mozilla_ios_lockbox_derived__baseline_clients_last_seen__v1_external",
        parent_group=task_group_lockwise_ios,
    ) as org_mozilla_ios_lockbox_derived__baseline_clients_last_seen__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_nondesktop__wait_for_org_mozilla_ios_lockbox_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_nondesktop",
            external_task_id="wait_for_org_mozilla_ios_lockbox_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=82800)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_gud__wait_for_org_mozilla_ios_lockbox_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_gud",
            external_task_id="wait_for_org_mozilla_ios_lockbox_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=82800)).isoformat() }}",
        )

        org_mozilla_ios_lockbox_derived__baseline_clients_last_seen__v1_external.set_upstream(
            org_mozilla_ios_lockbox_derived__baseline_clients_last_seen__v1
        )

    org_mozilla_klar_derived__baseline_clients_daily__v1 = bigquery_etl_query(
        task_id="org_mozilla_klar_derived__baseline_clients_daily__v1",
        destination_table="baseline_clients_daily_v1",
        dataset_id="org_mozilla_klar_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_klar_android,
    )

    org_mozilla_klar_derived__baseline_clients_first_seen__v1 = bigquery_etl_query(
        task_id="org_mozilla_klar_derived__baseline_clients_first_seen__v1",
        destination_table="baseline_clients_first_seen_v1",
        dataset_id="org_mozilla_klar_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=True,
        parameters=["submission_date:DATE:{{ds}}"],
        task_group=task_group_klar_android,
    )

    org_mozilla_klar_derived__baseline_clients_last_seen__v1 = bigquery_etl_query(
        task_id="org_mozilla_klar_derived__baseline_clients_last_seen__v1",
        destination_table="baseline_clients_last_seen_v1",
        dataset_id="org_mozilla_klar_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=True,
        task_group=task_group_klar_android,
    )

    with TaskGroup(
        "org_mozilla_klar_derived__baseline_clients_last_seen__v1_external",
        parent_group=task_group_klar_android,
    ) as org_mozilla_klar_derived__baseline_clients_last_seen__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_nondesktop__wait_for_org_mozilla_klar_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_nondesktop",
            external_task_id="wait_for_org_mozilla_klar_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=82800)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_gud__wait_for_org_mozilla_klar_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_gud",
            external_task_id="wait_for_org_mozilla_klar_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=82800)).isoformat() }}",
        )

        org_mozilla_klar_derived__baseline_clients_last_seen__v1_external.set_upstream(
            org_mozilla_klar_derived__baseline_clients_last_seen__v1
        )

    org_mozilla_mozregression_derived__baseline_clients_daily__v1 = bigquery_etl_query(
        task_id="org_mozilla_mozregression_derived__baseline_clients_daily__v1",
        destination_table="baseline_clients_daily_v1",
        dataset_id="org_mozilla_mozregression_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_mozregression,
    )

    org_mozilla_mozregression_derived__baseline_clients_first_seen__v1 = bigquery_etl_query(
        task_id="org_mozilla_mozregression_derived__baseline_clients_first_seen__v1",
        destination_table="baseline_clients_first_seen_v1",
        dataset_id="org_mozilla_mozregression_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=True,
        parameters=["submission_date:DATE:{{ds}}"],
        task_group=task_group_mozregression,
    )

    org_mozilla_mozregression_derived__baseline_clients_last_seen__v1 = (
        bigquery_etl_query(
            task_id="org_mozilla_mozregression_derived__baseline_clients_last_seen__v1",
            destination_table="baseline_clients_last_seen_v1",
            dataset_id="org_mozilla_mozregression_derived",
            project_id="moz-fx-data-shared-prod",
            owner="ascholtz@mozilla.com",
            email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
            date_partition_parameter="submission_date",
            depends_on_past=True,
            task_group=task_group_mozregression,
        )
    )

    org_mozilla_reference_browser_derived__baseline_clients_daily__v1 = (
        bigquery_etl_query(
            task_id="org_mozilla_reference_browser_derived__baseline_clients_daily__v1",
            destination_table="baseline_clients_daily_v1",
            dataset_id="org_mozilla_reference_browser_derived",
            project_id="moz-fx-data-shared-prod",
            owner="ascholtz@mozilla.com",
            email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
            date_partition_parameter="submission_date",
            depends_on_past=False,
            task_group=task_group_reference_browser,
        )
    )

    org_mozilla_reference_browser_derived__baseline_clients_first_seen__v1 = bigquery_etl_query(
        task_id="org_mozilla_reference_browser_derived__baseline_clients_first_seen__v1",
        destination_table="baseline_clients_first_seen_v1",
        dataset_id="org_mozilla_reference_browser_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=True,
        parameters=["submission_date:DATE:{{ds}}"],
        task_group=task_group_reference_browser,
    )

    org_mozilla_reference_browser_derived__baseline_clients_last_seen__v1 = bigquery_etl_query(
        task_id="org_mozilla_reference_browser_derived__baseline_clients_last_seen__v1",
        destination_table="baseline_clients_last_seen_v1",
        dataset_id="org_mozilla_reference_browser_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=True,
        task_group=task_group_reference_browser,
    )

    with TaskGroup(
        "org_mozilla_reference_browser_derived__baseline_clients_last_seen__v1_external",
        parent_group=task_group_reference_browser,
    ) as org_mozilla_reference_browser_derived__baseline_clients_last_seen__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_nondesktop__wait_for_org_mozilla_reference_browser_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_nondesktop",
            external_task_id="wait_for_org_mozilla_reference_browser_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=82800)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_gud__wait_for_org_mozilla_reference_browser_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_gud",
            external_task_id="wait_for_org_mozilla_reference_browser_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=82800)).isoformat() }}",
        )

        org_mozilla_reference_browser_derived__baseline_clients_last_seen__v1_external.set_upstream(
            org_mozilla_reference_browser_derived__baseline_clients_last_seen__v1
        )

    org_mozilla_tv_firefox_derived__baseline_clients_daily__v1 = bigquery_etl_query(
        task_id="org_mozilla_tv_firefox_derived__baseline_clients_daily__v1",
        destination_table="baseline_clients_daily_v1",
        dataset_id="org_mozilla_tv_firefox_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_firefox_fire_tv,
    )

    org_mozilla_tv_firefox_derived__baseline_clients_first_seen__v1 = (
        bigquery_etl_query(
            task_id="org_mozilla_tv_firefox_derived__baseline_clients_first_seen__v1",
            destination_table="baseline_clients_first_seen_v1",
            dataset_id="org_mozilla_tv_firefox_derived",
            project_id="moz-fx-data-shared-prod",
            owner="ascholtz@mozilla.com",
            email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
            date_partition_parameter=None,
            depends_on_past=True,
            parameters=["submission_date:DATE:{{ds}}"],
            task_group=task_group_firefox_fire_tv,
        )
    )

    org_mozilla_tv_firefox_derived__baseline_clients_last_seen__v1 = bigquery_etl_query(
        task_id="org_mozilla_tv_firefox_derived__baseline_clients_last_seen__v1",
        destination_table="baseline_clients_last_seen_v1",
        dataset_id="org_mozilla_tv_firefox_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=True,
        task_group=task_group_firefox_fire_tv,
    )

    with TaskGroup(
        "org_mozilla_tv_firefox_derived__baseline_clients_last_seen__v1_external",
        parent_group=task_group_firefox_fire_tv,
    ) as org_mozilla_tv_firefox_derived__baseline_clients_last_seen__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_nondesktop__wait_for_org_mozilla_tv_firefox_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_nondesktop",
            external_task_id="wait_for_org_mozilla_tv_firefox_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=82800)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_gud__wait_for_org_mozilla_tv_firefox_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_gud",
            external_task_id="wait_for_org_mozilla_tv_firefox_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=82800)).isoformat() }}",
        )

        org_mozilla_tv_firefox_derived__baseline_clients_last_seen__v1_external.set_upstream(
            org_mozilla_tv_firefox_derived__baseline_clients_last_seen__v1
        )

    org_mozilla_vrbrowser_derived__baseline_clients_daily__v1 = bigquery_etl_query(
        task_id="org_mozilla_vrbrowser_derived__baseline_clients_daily__v1",
        destination_table="baseline_clients_daily_v1",
        dataset_id="org_mozilla_vrbrowser_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_firefox_reality,
    )

    org_mozilla_vrbrowser_derived__baseline_clients_first_seen__v1 = bigquery_etl_query(
        task_id="org_mozilla_vrbrowser_derived__baseline_clients_first_seen__v1",
        destination_table="baseline_clients_first_seen_v1",
        dataset_id="org_mozilla_vrbrowser_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=True,
        parameters=["submission_date:DATE:{{ds}}"],
        task_group=task_group_firefox_reality,
    )

    org_mozilla_vrbrowser_derived__baseline_clients_last_seen__v1 = bigquery_etl_query(
        task_id="org_mozilla_vrbrowser_derived__baseline_clients_last_seen__v1",
        destination_table="baseline_clients_last_seen_v1",
        dataset_id="org_mozilla_vrbrowser_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=True,
        task_group=task_group_firefox_reality,
    )

    with TaskGroup(
        "org_mozilla_vrbrowser_derived__baseline_clients_last_seen__v1_external",
        parent_group=task_group_firefox_reality,
    ) as org_mozilla_vrbrowser_derived__baseline_clients_last_seen__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_nondesktop__wait_for_org_mozilla_vrbrowser_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_nondesktop",
            external_task_id="wait_for_org_mozilla_vrbrowser_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=82800)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_gud__wait_for_org_mozilla_vrbrowser_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_gud",
            external_task_id="wait_for_org_mozilla_vrbrowser_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=82800)).isoformat() }}",
        )

        org_mozilla_vrbrowser_derived__baseline_clients_last_seen__v1_external.set_upstream(
            org_mozilla_vrbrowser_derived__baseline_clients_last_seen__v1
        )

    pine_derived__baseline_clients_daily__v1 = bigquery_etl_query(
        task_id="pine_derived__baseline_clients_daily__v1",
        destination_table="baseline_clients_daily_v1",
        dataset_id="pine_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_pine,
    )

    pine_derived__baseline_clients_first_seen__v1 = bigquery_etl_query(
        task_id="pine_derived__baseline_clients_first_seen__v1",
        destination_table="baseline_clients_first_seen_v1",
        dataset_id="pine_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=True,
        parameters=["submission_date:DATE:{{ds}}"],
        task_group=task_group_pine,
    )

    pine_derived__baseline_clients_last_seen__v1 = bigquery_etl_query(
        task_id="pine_derived__baseline_clients_last_seen__v1",
        destination_table="baseline_clients_last_seen_v1",
        dataset_id="pine_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=True,
        task_group=task_group_pine,
    )

    pine_derived__metrics_clients_daily__v1 = bigquery_etl_query(
        task_id="pine_derived__metrics_clients_daily__v1",
        destination_table="metrics_clients_daily_v1",
        dataset_id="pine_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_pine,
    )

    reference_browser_derived__clients_last_seen_joined__v1 = bigquery_etl_query(
        task_id="reference_browser_derived__clients_last_seen_joined__v1",
        destination_table="clients_last_seen_joined_v1",
        dataset_id="reference_browser_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=True,
        task_group=task_group_reference_browser,
    )

    reference_browser_derived__metrics_clients_daily__v1 = bigquery_etl_query(
        task_id="reference_browser_derived__metrics_clients_daily__v1",
        destination_table="metrics_clients_daily_v1",
        dataset_id="reference_browser_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_reference_browser,
    )

    reference_browser_derived__metrics_clients_last_seen__v1 = bigquery_etl_query(
        task_id="reference_browser_derived__metrics_clients_last_seen__v1",
        destination_table="metrics_clients_last_seen_v1",
        dataset_id="reference_browser_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_reference_browser,
    )

    accounts_backend_derived__baseline_clients_daily__v1.set_upstream(
        accounts_backend_derived__baseline_clients_first_seen__v1
    )
    wait_for_copy_deduplicate_all = ExternalTaskSensor(
        task_id="wait_for_copy_deduplicate_all",
        external_dag_id="copy_deduplicate",
        external_task_id="copy_deduplicate_all",
        execution_delta=datetime.timedelta(seconds=3600),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    accounts_backend_derived__baseline_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    accounts_backend_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )
    wait_for_telemetry_derived__core_clients_first_seen__v1 = ExternalTaskSensor(
        task_id="wait_for_telemetry_derived__core_clients_first_seen__v1",
        external_dag_id="copy_deduplicate",
        external_task_id="telemetry_derived__core_clients_first_seen__v1",
        execution_delta=datetime.timedelta(seconds=3600),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    accounts_backend_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_telemetry_derived__core_clients_first_seen__v1
    )

    accounts_backend_derived__baseline_clients_last_seen__v1.set_upstream(
        accounts_backend_derived__baseline_clients_daily__v1
    )

    accounts_backend_derived__metrics_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    burnham_derived__baseline_clients_daily__v1.set_upstream(
        burnham_derived__baseline_clients_first_seen__v1
    )
    burnham_derived__baseline_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    burnham_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )
    burnham_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_telemetry_derived__core_clients_first_seen__v1
    )

    burnham_derived__baseline_clients_last_seen__v1.set_upstream(
        burnham_derived__baseline_clients_daily__v1
    )

    burnham_derived__clients_last_seen_joined__v1.set_upstream(
        burnham_derived__baseline_clients_last_seen__v1
    )

    burnham_derived__clients_last_seen_joined__v1.set_upstream(
        burnham_derived__metrics_clients_last_seen__v1
    )

    burnham_derived__metrics_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    burnham_derived__metrics_clients_last_seen__v1.set_upstream(
        burnham_derived__metrics_clients_daily__v1
    )

    checks__warn_firefox_desktop_background_defaultagent_derived__baseline_clients_last_seen__v1.set_upstream(
        firefox_desktop_background_defaultagent_derived__baseline_clients_last_seen__v1
    )

    checks__warn_firefox_desktop_background_tasks_derived__baseline_clients_last_seen__v1.set_upstream(
        firefox_desktop_background_tasks_derived__baseline_clients_last_seen__v1
    )

    checks__warn_firefox_desktop_background_update_derived__baseline_clients_last_seen__v1.set_upstream(
        firefox_desktop_background_update_derived__baseline_clients_last_seen__v1
    )

    checks__warn_firefox_desktop_derived__baseline_clients_last_seen__v1.set_upstream(
        firefox_desktop_derived__baseline_clients_last_seen__v1
    )

    checks__warn_org_mozilla_connect_firefox_derived__baseline_clients_last_seen__v1.set_upstream(
        org_mozilla_connect_firefox_derived__baseline_clients_last_seen__v1
    )

    checks__warn_org_mozilla_fenix_derived__baseline_clients_last_seen__v1.set_upstream(
        org_mozilla_fenix_derived__baseline_clients_last_seen__v1
    )

    checks__warn_org_mozilla_fenix_nightly_derived__baseline_clients_last_seen__v1.set_upstream(
        org_mozilla_fenix_nightly_derived__baseline_clients_last_seen__v1
    )

    checks__warn_org_mozilla_fennec_aurora_derived__baseline_clients_last_seen__v1.set_upstream(
        org_mozilla_fennec_aurora_derived__baseline_clients_last_seen__v1
    )

    checks__warn_org_mozilla_firefox_beta_derived__baseline_clients_last_seen__v1.set_upstream(
        org_mozilla_firefox_beta_derived__baseline_clients_last_seen__v1
    )

    checks__warn_org_mozilla_firefox_derived__baseline_clients_last_seen__v1.set_upstream(
        org_mozilla_firefox_derived__baseline_clients_last_seen__v1
    )

    checks__warn_org_mozilla_focus_beta_derived__baseline_clients_last_seen__v1.set_upstream(
        org_mozilla_focus_beta_derived__baseline_clients_last_seen__v1
    )

    checks__warn_org_mozilla_focus_derived__baseline_clients_last_seen__v1.set_upstream(
        org_mozilla_focus_derived__baseline_clients_last_seen__v1
    )

    checks__warn_org_mozilla_focus_nightly_derived__baseline_clients_last_seen__v1.set_upstream(
        org_mozilla_focus_nightly_derived__baseline_clients_last_seen__v1
    )

    checks__warn_org_mozilla_ios_fennec_derived__baseline_clients_last_seen__v1.set_upstream(
        org_mozilla_ios_fennec_derived__baseline_clients_last_seen__v1
    )

    checks__warn_org_mozilla_ios_firefox_derived__baseline_clients_last_seen__v1.set_upstream(
        org_mozilla_ios_firefox_derived__baseline_clients_last_seen__v1
    )

    checks__warn_org_mozilla_ios_firefoxbeta_derived__baseline_clients_last_seen__v1.set_upstream(
        org_mozilla_ios_firefoxbeta_derived__baseline_clients_last_seen__v1
    )

    checks__warn_org_mozilla_ios_focus_derived__baseline_clients_last_seen__v1.set_upstream(
        org_mozilla_ios_focus_derived__baseline_clients_last_seen__v1
    )

    checks__warn_org_mozilla_ios_klar_derived__baseline_clients_last_seen__v1.set_upstream(
        org_mozilla_ios_klar_derived__baseline_clients_last_seen__v1
    )

    checks__warn_org_mozilla_ios_lockbox_derived__baseline_clients_last_seen__v1.set_upstream(
        org_mozilla_ios_lockbox_derived__baseline_clients_last_seen__v1
    )

    checks__warn_org_mozilla_klar_derived__baseline_clients_last_seen__v1.set_upstream(
        org_mozilla_klar_derived__baseline_clients_last_seen__v1
    )

    checks__warn_org_mozilla_reference_browser_derived__baseline_clients_last_seen__v1.set_upstream(
        org_mozilla_reference_browser_derived__baseline_clients_last_seen__v1
    )

    checks__warn_org_mozilla_vrbrowser_derived__baseline_clients_last_seen__v1.set_upstream(
        org_mozilla_vrbrowser_derived__baseline_clients_last_seen__v1
    )

    checks__warn_pine_derived__baseline_clients_last_seen__v1.set_upstream(
        pine_derived__baseline_clients_last_seen__v1
    )

    fenix_derived__clients_last_seen_joined__v1.set_upstream(
        fenix_derived__metrics_clients_last_seen__v1
    )

    fenix_derived__clients_last_seen_joined__v1.set_upstream(
        org_mozilla_fenix_derived__baseline_clients_last_seen__v1
    )

    fenix_derived__clients_last_seen_joined__v1.set_upstream(
        org_mozilla_fenix_nightly_derived__baseline_clients_last_seen__v1
    )

    fenix_derived__clients_last_seen_joined__v1.set_upstream(
        org_mozilla_fennec_aurora_derived__baseline_clients_last_seen__v1
    )

    fenix_derived__clients_last_seen_joined__v1.set_upstream(
        org_mozilla_firefox_beta_derived__baseline_clients_last_seen__v1
    )

    fenix_derived__clients_last_seen_joined__v1.set_upstream(
        org_mozilla_firefox_derived__baseline_clients_last_seen__v1
    )

    fenix_derived__metrics_clients_daily__v1.set_upstream(wait_for_copy_deduplicate_all)

    fenix_derived__metrics_clients_last_seen__v1.set_upstream(
        fenix_derived__metrics_clients_daily__v1
    )

    firefox_desktop_background_defaultagent_derived__baseline_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    firefox_desktop_background_defaultagent_derived__baseline_clients_daily__v1.set_upstream(
        firefox_desktop_background_defaultagent_derived__baseline_clients_first_seen__v1
    )

    firefox_desktop_background_defaultagent_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )
    firefox_desktop_background_defaultagent_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_telemetry_derived__core_clients_first_seen__v1
    )

    firefox_desktop_background_defaultagent_derived__baseline_clients_last_seen__v1.set_upstream(
        firefox_desktop_background_defaultagent_derived__baseline_clients_daily__v1
    )

    firefox_desktop_background_defaultagent_derived__metrics_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    firefox_desktop_background_tasks_derived__baseline_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    firefox_desktop_background_tasks_derived__baseline_clients_daily__v1.set_upstream(
        firefox_desktop_background_tasks_derived__baseline_clients_first_seen__v1
    )

    firefox_desktop_background_tasks_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )
    firefox_desktop_background_tasks_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_telemetry_derived__core_clients_first_seen__v1
    )

    firefox_desktop_background_tasks_derived__baseline_clients_last_seen__v1.set_upstream(
        firefox_desktop_background_tasks_derived__baseline_clients_daily__v1
    )

    firefox_desktop_background_tasks_derived__metrics_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    firefox_desktop_background_update_derived__baseline_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    firefox_desktop_background_update_derived__baseline_clients_daily__v1.set_upstream(
        firefox_desktop_background_update_derived__baseline_clients_first_seen__v1
    )

    firefox_desktop_background_update_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )
    firefox_desktop_background_update_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_telemetry_derived__core_clients_first_seen__v1
    )

    firefox_desktop_background_update_derived__baseline_clients_last_seen__v1.set_upstream(
        firefox_desktop_background_update_derived__baseline_clients_daily__v1
    )

    firefox_desktop_background_update_derived__clients_last_seen_joined__v1.set_upstream(
        firefox_desktop_background_update_derived__baseline_clients_last_seen__v1
    )

    firefox_desktop_background_update_derived__clients_last_seen_joined__v1.set_upstream(
        firefox_desktop_background_update_derived__metrics_clients_last_seen__v1
    )

    firefox_desktop_background_update_derived__metrics_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    firefox_desktop_background_update_derived__metrics_clients_last_seen__v1.set_upstream(
        firefox_desktop_background_update_derived__metrics_clients_daily__v1
    )

    firefox_desktop_derived__baseline_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    firefox_desktop_derived__baseline_clients_daily__v1.set_upstream(
        firefox_desktop_derived__baseline_clients_first_seen__v1
    )

    firefox_desktop_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )
    firefox_desktop_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_telemetry_derived__core_clients_first_seen__v1
    )

    firefox_desktop_derived__baseline_clients_last_seen__v1.set_upstream(
        firefox_desktop_derived__baseline_clients_daily__v1
    )

    firefox_desktop_derived__clients_last_seen_joined__v1.set_upstream(
        firefox_desktop_derived__baseline_clients_last_seen__v1
    )

    firefox_desktop_derived__clients_last_seen_joined__v1.set_upstream(
        firefox_desktop_derived__metrics_clients_last_seen__v1
    )

    firefox_desktop_derived__metrics_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    firefox_desktop_derived__metrics_clients_last_seen__v1.set_upstream(
        firefox_desktop_derived__metrics_clients_daily__v1
    )

    firefox_echo_show_derived__clients_last_seen_joined__v1.set_upstream(
        firefox_echo_show_derived__metrics_clients_last_seen__v1
    )

    firefox_echo_show_derived__clients_last_seen_joined__v1.set_upstream(
        org_mozilla_connect_firefox_derived__baseline_clients_last_seen__v1
    )

    firefox_echo_show_derived__metrics_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    firefox_echo_show_derived__metrics_clients_last_seen__v1.set_upstream(
        firefox_echo_show_derived__metrics_clients_daily__v1
    )

    firefox_fire_tv_derived__clients_last_seen_joined__v1.set_upstream(
        firefox_fire_tv_derived__metrics_clients_last_seen__v1
    )

    firefox_fire_tv_derived__clients_last_seen_joined__v1.set_upstream(
        org_mozilla_tv_firefox_derived__baseline_clients_last_seen__v1
    )

    firefox_fire_tv_derived__metrics_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    firefox_fire_tv_derived__metrics_clients_last_seen__v1.set_upstream(
        firefox_fire_tv_derived__metrics_clients_daily__v1
    )

    firefox_ios_derived__clients_last_seen_joined__v1.set_upstream(
        firefox_ios_derived__metrics_clients_last_seen__v1
    )

    firefox_ios_derived__clients_last_seen_joined__v1.set_upstream(
        org_mozilla_ios_fennec_derived__baseline_clients_last_seen__v1
    )

    firefox_ios_derived__clients_last_seen_joined__v1.set_upstream(
        org_mozilla_ios_firefox_derived__baseline_clients_last_seen__v1
    )

    firefox_ios_derived__clients_last_seen_joined__v1.set_upstream(
        org_mozilla_ios_firefoxbeta_derived__baseline_clients_last_seen__v1
    )

    firefox_ios_derived__metrics_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    firefox_ios_derived__metrics_clients_last_seen__v1.set_upstream(
        firefox_ios_derived__metrics_clients_daily__v1
    )

    firefox_reality_derived__clients_last_seen_joined__v1.set_upstream(
        firefox_reality_derived__metrics_clients_last_seen__v1
    )

    firefox_reality_derived__clients_last_seen_joined__v1.set_upstream(
        org_mozilla_vrbrowser_derived__baseline_clients_last_seen__v1
    )

    firefox_reality_derived__metrics_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    firefox_reality_derived__metrics_clients_last_seen__v1.set_upstream(
        firefox_reality_derived__metrics_clients_daily__v1
    )

    firefox_reality_pc_derived__clients_last_seen_joined__v1.set_upstream(
        firefox_reality_pc_derived__metrics_clients_last_seen__v1
    )

    firefox_reality_pc_derived__clients_last_seen_joined__v1.set_upstream(
        org_mozilla_firefoxreality_derived__baseline_clients_last_seen__v1
    )

    firefox_reality_pc_derived__metrics_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    firefox_reality_pc_derived__metrics_clients_last_seen__v1.set_upstream(
        firefox_reality_pc_derived__metrics_clients_daily__v1
    )

    focus_android_derived__clients_last_seen_joined__v1.set_upstream(
        focus_android_derived__metrics_clients_last_seen__v1
    )

    focus_android_derived__clients_last_seen_joined__v1.set_upstream(
        org_mozilla_focus_beta_derived__baseline_clients_last_seen__v1
    )

    focus_android_derived__clients_last_seen_joined__v1.set_upstream(
        org_mozilla_focus_derived__baseline_clients_last_seen__v1
    )

    focus_android_derived__clients_last_seen_joined__v1.set_upstream(
        org_mozilla_focus_nightly_derived__baseline_clients_last_seen__v1
    )

    focus_android_derived__metrics_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    focus_android_derived__metrics_clients_last_seen__v1.set_upstream(
        focus_android_derived__metrics_clients_daily__v1
    )

    focus_ios_derived__clients_last_seen_joined__v1.set_upstream(
        focus_ios_derived__metrics_clients_last_seen__v1
    )

    focus_ios_derived__clients_last_seen_joined__v1.set_upstream(
        org_mozilla_ios_focus_derived__baseline_clients_last_seen__v1
    )

    focus_ios_derived__metrics_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    focus_ios_derived__metrics_clients_last_seen__v1.set_upstream(
        focus_ios_derived__metrics_clients_daily__v1
    )

    klar_android_derived__clients_last_seen_joined__v1.set_upstream(
        klar_android_derived__metrics_clients_last_seen__v1
    )

    klar_android_derived__clients_last_seen_joined__v1.set_upstream(
        org_mozilla_klar_derived__baseline_clients_last_seen__v1
    )

    klar_android_derived__metrics_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    klar_android_derived__metrics_clients_last_seen__v1.set_upstream(
        klar_android_derived__metrics_clients_daily__v1
    )

    klar_ios_derived__clients_last_seen_joined__v1.set_upstream(
        klar_ios_derived__metrics_clients_last_seen__v1
    )

    klar_ios_derived__clients_last_seen_joined__v1.set_upstream(
        org_mozilla_ios_klar_derived__baseline_clients_last_seen__v1
    )

    klar_ios_derived__metrics_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    klar_ios_derived__metrics_clients_last_seen__v1.set_upstream(
        klar_ios_derived__metrics_clients_daily__v1
    )

    lockwise_android_derived__clients_last_seen_joined__v1.set_upstream(
        lockwise_android_derived__metrics_clients_last_seen__v1
    )

    lockwise_android_derived__clients_last_seen_joined__v1.set_upstream(
        mozilla_lockbox_derived__baseline_clients_last_seen__v1
    )

    lockwise_android_derived__metrics_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    lockwise_android_derived__metrics_clients_last_seen__v1.set_upstream(
        lockwise_android_derived__metrics_clients_daily__v1
    )

    lockwise_ios_derived__clients_last_seen_joined__v1.set_upstream(
        lockwise_ios_derived__metrics_clients_last_seen__v1
    )

    lockwise_ios_derived__clients_last_seen_joined__v1.set_upstream(
        org_mozilla_ios_lockbox_derived__baseline_clients_last_seen__v1
    )

    lockwise_ios_derived__metrics_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    lockwise_ios_derived__metrics_clients_last_seen__v1.set_upstream(
        lockwise_ios_derived__metrics_clients_daily__v1
    )

    mach_derived__clients_last_seen_joined__v1.set_upstream(
        mach_derived__metrics_clients_last_seen__v1
    )

    mach_derived__clients_last_seen_joined__v1.set_upstream(
        mozilla_mach_derived__baseline_clients_last_seen__v1
    )

    mach_derived__metrics_clients_daily__v1.set_upstream(wait_for_copy_deduplicate_all)

    mach_derived__metrics_clients_last_seen__v1.set_upstream(
        mach_derived__metrics_clients_daily__v1
    )

    monitor_cirrus_derived__baseline_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    monitor_cirrus_derived__baseline_clients_daily__v1.set_upstream(
        monitor_cirrus_derived__baseline_clients_first_seen__v1
    )

    monitor_cirrus_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )
    monitor_cirrus_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_telemetry_derived__core_clients_first_seen__v1
    )

    monitor_cirrus_derived__baseline_clients_last_seen__v1.set_upstream(
        monitor_cirrus_derived__baseline_clients_daily__v1
    )

    monitor_cirrus_derived__metrics_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    moso_mastodon_backend_derived__baseline_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    moso_mastodon_backend_derived__baseline_clients_daily__v1.set_upstream(
        moso_mastodon_backend_derived__baseline_clients_first_seen__v1
    )

    moso_mastodon_backend_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )
    moso_mastodon_backend_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_telemetry_derived__core_clients_first_seen__v1
    )

    moso_mastodon_backend_derived__baseline_clients_last_seen__v1.set_upstream(
        moso_mastodon_backend_derived__baseline_clients_daily__v1
    )

    moso_mastodon_backend_derived__metrics_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    mozilla_lockbox_derived__baseline_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    mozilla_lockbox_derived__baseline_clients_daily__v1.set_upstream(
        mozilla_lockbox_derived__baseline_clients_first_seen__v1
    )

    mozilla_lockbox_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )
    mozilla_lockbox_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_telemetry_derived__core_clients_first_seen__v1
    )

    mozilla_lockbox_derived__baseline_clients_last_seen__v1.set_upstream(
        mozilla_lockbox_derived__baseline_clients_daily__v1
    )

    mozilla_mach_derived__baseline_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    mozilla_mach_derived__baseline_clients_daily__v1.set_upstream(
        mozilla_mach_derived__baseline_clients_first_seen__v1
    )

    mozilla_mach_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )
    mozilla_mach_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_telemetry_derived__core_clients_first_seen__v1
    )

    mozilla_mach_derived__baseline_clients_last_seen__v1.set_upstream(
        mozilla_mach_derived__baseline_clients_daily__v1
    )

    mozilla_vpn_derived__metrics_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    mozillavpn_cirrus_derived__baseline_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    mozillavpn_cirrus_derived__baseline_clients_daily__v1.set_upstream(
        mozillavpn_cirrus_derived__baseline_clients_first_seen__v1
    )

    mozillavpn_cirrus_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )
    mozillavpn_cirrus_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_telemetry_derived__core_clients_first_seen__v1
    )

    mozillavpn_cirrus_derived__baseline_clients_last_seen__v1.set_upstream(
        mozillavpn_cirrus_derived__baseline_clients_daily__v1
    )

    mozillavpn_cirrus_derived__metrics_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    mozillavpn_derived__baseline_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    mozillavpn_derived__baseline_clients_daily__v1.set_upstream(
        mozillavpn_derived__baseline_clients_first_seen__v1
    )

    mozillavpn_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )
    mozillavpn_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_telemetry_derived__core_clients_first_seen__v1
    )

    mozillavpn_derived__baseline_clients_last_seen__v1.set_upstream(
        mozillavpn_derived__baseline_clients_daily__v1
    )

    mozphab_derived__baseline_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    mozphab_derived__baseline_clients_daily__v1.set_upstream(
        mozphab_derived__baseline_clients_first_seen__v1
    )

    mozphab_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )
    mozphab_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_telemetry_derived__core_clients_first_seen__v1
    )

    mozphab_derived__baseline_clients_last_seen__v1.set_upstream(
        mozphab_derived__baseline_clients_daily__v1
    )

    mozphab_derived__clients_last_seen_joined__v1.set_upstream(
        mozphab_derived__baseline_clients_last_seen__v1
    )

    mozphab_derived__clients_last_seen_joined__v1.set_upstream(
        mozphab_derived__metrics_clients_last_seen__v1
    )

    mozphab_derived__metrics_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    mozphab_derived__metrics_clients_last_seen__v1.set_upstream(
        mozphab_derived__metrics_clients_daily__v1
    )

    mozregression_derived__clients_last_seen_joined__v1.set_upstream(
        mozregression_derived__metrics_clients_last_seen__v1
    )

    mozregression_derived__clients_last_seen_joined__v1.set_upstream(
        org_mozilla_mozregression_derived__baseline_clients_last_seen__v1
    )

    mozregression_derived__metrics_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    mozregression_derived__metrics_clients_last_seen__v1.set_upstream(
        mozregression_derived__metrics_clients_daily__v1
    )

    org_mozilla_connect_firefox_derived__baseline_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    org_mozilla_connect_firefox_derived__baseline_clients_daily__v1.set_upstream(
        org_mozilla_connect_firefox_derived__baseline_clients_first_seen__v1
    )

    org_mozilla_connect_firefox_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )
    org_mozilla_connect_firefox_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_telemetry_derived__core_clients_first_seen__v1
    )

    org_mozilla_connect_firefox_derived__baseline_clients_last_seen__v1.set_upstream(
        org_mozilla_connect_firefox_derived__baseline_clients_daily__v1
    )

    org_mozilla_fenix_derived__baseline_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    org_mozilla_fenix_derived__baseline_clients_daily__v1.set_upstream(
        org_mozilla_fenix_derived__baseline_clients_first_seen__v1
    )

    org_mozilla_fenix_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )
    org_mozilla_fenix_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_telemetry_derived__core_clients_first_seen__v1
    )

    org_mozilla_fenix_derived__baseline_clients_last_seen__v1.set_upstream(
        org_mozilla_fenix_derived__baseline_clients_daily__v1
    )

    org_mozilla_fenix_nightly_derived__baseline_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    org_mozilla_fenix_nightly_derived__baseline_clients_daily__v1.set_upstream(
        org_mozilla_fenix_nightly_derived__baseline_clients_first_seen__v1
    )

    org_mozilla_fenix_nightly_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )
    org_mozilla_fenix_nightly_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_telemetry_derived__core_clients_first_seen__v1
    )

    org_mozilla_fenix_nightly_derived__baseline_clients_last_seen__v1.set_upstream(
        org_mozilla_fenix_nightly_derived__baseline_clients_daily__v1
    )

    org_mozilla_fennec_aurora_derived__baseline_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    org_mozilla_fennec_aurora_derived__baseline_clients_daily__v1.set_upstream(
        org_mozilla_fennec_aurora_derived__baseline_clients_first_seen__v1
    )

    org_mozilla_fennec_aurora_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )
    org_mozilla_fennec_aurora_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_telemetry_derived__core_clients_first_seen__v1
    )

    org_mozilla_fennec_aurora_derived__baseline_clients_last_seen__v1.set_upstream(
        org_mozilla_fennec_aurora_derived__baseline_clients_daily__v1
    )

    org_mozilla_firefox_beta_derived__baseline_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    org_mozilla_firefox_beta_derived__baseline_clients_daily__v1.set_upstream(
        org_mozilla_firefox_beta_derived__baseline_clients_first_seen__v1
    )

    org_mozilla_firefox_beta_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )
    org_mozilla_firefox_beta_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_telemetry_derived__core_clients_first_seen__v1
    )

    org_mozilla_firefox_beta_derived__baseline_clients_last_seen__v1.set_upstream(
        org_mozilla_firefox_beta_derived__baseline_clients_daily__v1
    )

    org_mozilla_firefox_derived__baseline_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    org_mozilla_firefox_derived__baseline_clients_daily__v1.set_upstream(
        org_mozilla_firefox_derived__baseline_clients_first_seen__v1
    )

    org_mozilla_firefox_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )
    org_mozilla_firefox_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_telemetry_derived__core_clients_first_seen__v1
    )

    org_mozilla_firefox_derived__baseline_clients_last_seen__v1.set_upstream(
        org_mozilla_firefox_derived__baseline_clients_daily__v1
    )

    org_mozilla_firefox_vpn_derived__baseline_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    org_mozilla_firefox_vpn_derived__baseline_clients_daily__v1.set_upstream(
        org_mozilla_firefox_vpn_derived__baseline_clients_first_seen__v1
    )

    org_mozilla_firefox_vpn_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )
    org_mozilla_firefox_vpn_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_telemetry_derived__core_clients_first_seen__v1
    )

    org_mozilla_firefox_vpn_derived__baseline_clients_last_seen__v1.set_upstream(
        org_mozilla_firefox_vpn_derived__baseline_clients_daily__v1
    )

    org_mozilla_firefoxreality_derived__baseline_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    org_mozilla_firefoxreality_derived__baseline_clients_daily__v1.set_upstream(
        org_mozilla_firefoxreality_derived__baseline_clients_first_seen__v1
    )

    org_mozilla_firefoxreality_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )
    org_mozilla_firefoxreality_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_telemetry_derived__core_clients_first_seen__v1
    )

    org_mozilla_firefoxreality_derived__baseline_clients_last_seen__v1.set_upstream(
        org_mozilla_firefoxreality_derived__baseline_clients_daily__v1
    )

    org_mozilla_focus_beta_derived__baseline_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    org_mozilla_focus_beta_derived__baseline_clients_daily__v1.set_upstream(
        org_mozilla_focus_beta_derived__baseline_clients_first_seen__v1
    )

    org_mozilla_focus_beta_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )
    org_mozilla_focus_beta_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_telemetry_derived__core_clients_first_seen__v1
    )

    org_mozilla_focus_beta_derived__baseline_clients_last_seen__v1.set_upstream(
        org_mozilla_focus_beta_derived__baseline_clients_daily__v1
    )

    org_mozilla_focus_derived__baseline_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    org_mozilla_focus_derived__baseline_clients_daily__v1.set_upstream(
        org_mozilla_focus_derived__baseline_clients_first_seen__v1
    )

    org_mozilla_focus_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )
    org_mozilla_focus_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_telemetry_derived__core_clients_first_seen__v1
    )

    org_mozilla_focus_derived__baseline_clients_last_seen__v1.set_upstream(
        org_mozilla_focus_derived__baseline_clients_daily__v1
    )

    org_mozilla_focus_nightly_derived__baseline_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    org_mozilla_focus_nightly_derived__baseline_clients_daily__v1.set_upstream(
        org_mozilla_focus_nightly_derived__baseline_clients_first_seen__v1
    )

    org_mozilla_focus_nightly_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )
    org_mozilla_focus_nightly_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_telemetry_derived__core_clients_first_seen__v1
    )

    org_mozilla_focus_nightly_derived__baseline_clients_last_seen__v1.set_upstream(
        org_mozilla_focus_nightly_derived__baseline_clients_daily__v1
    )

    org_mozilla_ios_fennec_derived__baseline_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    org_mozilla_ios_fennec_derived__baseline_clients_daily__v1.set_upstream(
        org_mozilla_ios_fennec_derived__baseline_clients_first_seen__v1
    )

    org_mozilla_ios_fennec_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )
    org_mozilla_ios_fennec_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_telemetry_derived__core_clients_first_seen__v1
    )

    org_mozilla_ios_fennec_derived__baseline_clients_last_seen__v1.set_upstream(
        org_mozilla_ios_fennec_derived__baseline_clients_daily__v1
    )

    org_mozilla_ios_firefox_derived__baseline_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    org_mozilla_ios_firefox_derived__baseline_clients_daily__v1.set_upstream(
        org_mozilla_ios_firefox_derived__baseline_clients_first_seen__v1
    )

    org_mozilla_ios_firefox_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )
    org_mozilla_ios_firefox_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_telemetry_derived__core_clients_first_seen__v1
    )

    org_mozilla_ios_firefox_derived__baseline_clients_last_seen__v1.set_upstream(
        org_mozilla_ios_firefox_derived__baseline_clients_daily__v1
    )

    org_mozilla_ios_firefoxbeta_derived__baseline_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    org_mozilla_ios_firefoxbeta_derived__baseline_clients_daily__v1.set_upstream(
        org_mozilla_ios_firefoxbeta_derived__baseline_clients_first_seen__v1
    )

    org_mozilla_ios_firefoxbeta_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )
    org_mozilla_ios_firefoxbeta_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_telemetry_derived__core_clients_first_seen__v1
    )

    org_mozilla_ios_firefoxbeta_derived__baseline_clients_last_seen__v1.set_upstream(
        org_mozilla_ios_firefoxbeta_derived__baseline_clients_daily__v1
    )

    org_mozilla_ios_firefoxvpn_derived__baseline_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    org_mozilla_ios_firefoxvpn_derived__baseline_clients_daily__v1.set_upstream(
        org_mozilla_ios_firefoxvpn_derived__baseline_clients_first_seen__v1
    )

    org_mozilla_ios_firefoxvpn_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )
    org_mozilla_ios_firefoxvpn_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_telemetry_derived__core_clients_first_seen__v1
    )

    org_mozilla_ios_firefoxvpn_derived__baseline_clients_last_seen__v1.set_upstream(
        org_mozilla_ios_firefoxvpn_derived__baseline_clients_daily__v1
    )

    org_mozilla_ios_firefoxvpn_network_extension_derived__baseline_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    org_mozilla_ios_firefoxvpn_network_extension_derived__baseline_clients_daily__v1.set_upstream(
        org_mozilla_ios_firefoxvpn_network_extension_derived__baseline_clients_first_seen__v1
    )

    org_mozilla_ios_firefoxvpn_network_extension_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )
    org_mozilla_ios_firefoxvpn_network_extension_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_telemetry_derived__core_clients_first_seen__v1
    )

    org_mozilla_ios_firefoxvpn_network_extension_derived__baseline_clients_last_seen__v1.set_upstream(
        org_mozilla_ios_firefoxvpn_network_extension_derived__baseline_clients_daily__v1
    )

    org_mozilla_ios_focus_derived__baseline_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    org_mozilla_ios_focus_derived__baseline_clients_daily__v1.set_upstream(
        org_mozilla_ios_focus_derived__baseline_clients_first_seen__v1
    )

    org_mozilla_ios_focus_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )
    org_mozilla_ios_focus_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_telemetry_derived__core_clients_first_seen__v1
    )

    org_mozilla_ios_focus_derived__baseline_clients_last_seen__v1.set_upstream(
        org_mozilla_ios_focus_derived__baseline_clients_daily__v1
    )

    org_mozilla_ios_klar_derived__baseline_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    org_mozilla_ios_klar_derived__baseline_clients_daily__v1.set_upstream(
        org_mozilla_ios_klar_derived__baseline_clients_first_seen__v1
    )

    org_mozilla_ios_klar_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )
    org_mozilla_ios_klar_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_telemetry_derived__core_clients_first_seen__v1
    )

    org_mozilla_ios_klar_derived__baseline_clients_last_seen__v1.set_upstream(
        org_mozilla_ios_klar_derived__baseline_clients_daily__v1
    )

    org_mozilla_ios_lockbox_derived__baseline_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    org_mozilla_ios_lockbox_derived__baseline_clients_daily__v1.set_upstream(
        org_mozilla_ios_lockbox_derived__baseline_clients_first_seen__v1
    )

    org_mozilla_ios_lockbox_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )
    org_mozilla_ios_lockbox_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_telemetry_derived__core_clients_first_seen__v1
    )

    org_mozilla_ios_lockbox_derived__baseline_clients_last_seen__v1.set_upstream(
        org_mozilla_ios_lockbox_derived__baseline_clients_daily__v1
    )

    org_mozilla_klar_derived__baseline_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    org_mozilla_klar_derived__baseline_clients_daily__v1.set_upstream(
        org_mozilla_klar_derived__baseline_clients_first_seen__v1
    )

    org_mozilla_klar_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )
    org_mozilla_klar_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_telemetry_derived__core_clients_first_seen__v1
    )

    org_mozilla_klar_derived__baseline_clients_last_seen__v1.set_upstream(
        org_mozilla_klar_derived__baseline_clients_daily__v1
    )

    org_mozilla_mozregression_derived__baseline_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    org_mozilla_mozregression_derived__baseline_clients_daily__v1.set_upstream(
        org_mozilla_mozregression_derived__baseline_clients_first_seen__v1
    )

    org_mozilla_mozregression_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )
    org_mozilla_mozregression_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_telemetry_derived__core_clients_first_seen__v1
    )

    org_mozilla_mozregression_derived__baseline_clients_last_seen__v1.set_upstream(
        org_mozilla_mozregression_derived__baseline_clients_daily__v1
    )

    org_mozilla_reference_browser_derived__baseline_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    org_mozilla_reference_browser_derived__baseline_clients_daily__v1.set_upstream(
        org_mozilla_reference_browser_derived__baseline_clients_first_seen__v1
    )

    org_mozilla_reference_browser_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )
    org_mozilla_reference_browser_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_telemetry_derived__core_clients_first_seen__v1
    )

    org_mozilla_reference_browser_derived__baseline_clients_last_seen__v1.set_upstream(
        org_mozilla_reference_browser_derived__baseline_clients_daily__v1
    )

    org_mozilla_tv_firefox_derived__baseline_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    org_mozilla_tv_firefox_derived__baseline_clients_daily__v1.set_upstream(
        org_mozilla_tv_firefox_derived__baseline_clients_first_seen__v1
    )

    org_mozilla_tv_firefox_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )
    org_mozilla_tv_firefox_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_telemetry_derived__core_clients_first_seen__v1
    )

    org_mozilla_tv_firefox_derived__baseline_clients_last_seen__v1.set_upstream(
        org_mozilla_tv_firefox_derived__baseline_clients_daily__v1
    )

    org_mozilla_vrbrowser_derived__baseline_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    org_mozilla_vrbrowser_derived__baseline_clients_daily__v1.set_upstream(
        org_mozilla_vrbrowser_derived__baseline_clients_first_seen__v1
    )

    org_mozilla_vrbrowser_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )
    org_mozilla_vrbrowser_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_telemetry_derived__core_clients_first_seen__v1
    )

    org_mozilla_vrbrowser_derived__baseline_clients_last_seen__v1.set_upstream(
        org_mozilla_vrbrowser_derived__baseline_clients_daily__v1
    )

    pine_derived__baseline_clients_daily__v1.set_upstream(wait_for_copy_deduplicate_all)

    pine_derived__baseline_clients_daily__v1.set_upstream(
        pine_derived__baseline_clients_first_seen__v1
    )

    pine_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )
    pine_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_telemetry_derived__core_clients_first_seen__v1
    )

    pine_derived__baseline_clients_last_seen__v1.set_upstream(
        pine_derived__baseline_clients_daily__v1
    )

    pine_derived__metrics_clients_daily__v1.set_upstream(wait_for_copy_deduplicate_all)

    reference_browser_derived__clients_last_seen_joined__v1.set_upstream(
        org_mozilla_reference_browser_derived__baseline_clients_last_seen__v1
    )

    reference_browser_derived__clients_last_seen_joined__v1.set_upstream(
        reference_browser_derived__metrics_clients_last_seen__v1
    )

    reference_browser_derived__metrics_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    reference_browser_derived__metrics_clients_last_seen__v1.set_upstream(
        reference_browser_derived__metrics_clients_daily__v1
    )
