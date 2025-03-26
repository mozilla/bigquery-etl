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
### bqetl_glean_usage

Built from bigquery-etl repo, [`dags/bqetl_glean_usage.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_glean_usage.py)

#### Owner

ascholtz@mozilla.com

#### Tags

* impact/tier_1
* repo/bigquery-etl
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
    catchup=False,
) as dag:

    task_group_accounts_backend = TaskGroup("accounts_backend")

    task_group_accounts_cirrus = TaskGroup("accounts_cirrus")

    task_group_accounts_frontend = TaskGroup("accounts_frontend")

    task_group_ads_backend = TaskGroup("ads_backend")

    task_group_bedrock = TaskGroup("bedrock")

    task_group_bergamot = TaskGroup("bergamot")

    task_group_burnham = TaskGroup("burnham")

    task_group_debug_ping_view = TaskGroup("debug_ping_view")

    task_group_fenix = TaskGroup("fenix")

    task_group_firefox_crashreporter = TaskGroup("firefox_crashreporter")

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

    task_group_firefox_translations = TaskGroup("firefox_translations")

    task_group_focus_android = TaskGroup("focus_android")

    task_group_focus_ios = TaskGroup("focus_ios")

    task_group_glean_dictionary = TaskGroup("glean_dictionary")

    task_group_gleanjs_docs = TaskGroup("gleanjs_docs")

    task_group_klar_android = TaskGroup("klar_android")

    task_group_klar_ios = TaskGroup("klar_ios")

    task_group_lockwise_android = TaskGroup("lockwise_android")

    task_group_lockwise_ios = TaskGroup("lockwise_ios")

    task_group_mach = TaskGroup("mach")

    task_group_mdn_yari = TaskGroup("mdn_yari")

    task_group_monitor_backend = TaskGroup("monitor_backend")

    task_group_monitor_cirrus = TaskGroup("monitor_cirrus")

    task_group_monitor_frontend = TaskGroup("monitor_frontend")

    task_group_mozilla_vpn = TaskGroup("mozilla_vpn")

    task_group_mozillavpn_backend_cirrus = TaskGroup("mozillavpn_backend_cirrus")

    task_group_mozphab = TaskGroup("mozphab")

    task_group_mozregression = TaskGroup("mozregression")

    task_group_org_mozilla_social_nightly = TaskGroup("org_mozilla_social_nightly")

    task_group_pine = TaskGroup("pine")

    task_group_reference_browser = TaskGroup("reference_browser")

    task_group_relay_backend = TaskGroup("relay_backend")

    task_group_syncstorage = TaskGroup("syncstorage")

    task_group_thunderbird_android = TaskGroup("thunderbird_android")

    task_group_thunderbird_desktop = TaskGroup("thunderbird_desktop")

    task_group_treeherder = TaskGroup("treeherder")

    task_group_viu_politica = TaskGroup("viu_politica")

    wait_for_copy_deduplicate_all = ExternalTaskSensor(
        task_id="wait_for_copy_deduplicate_all",
        external_dag_id="copy_deduplicate",
        external_task_id="copy_deduplicate_all",
        execution_delta=datetime.timedelta(seconds=3600),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_telemetry_derived__core_clients_first_seen__v1 = ExternalTaskSensor(
        task_id="wait_for_telemetry_derived__core_clients_first_seen__v1",
        external_dag_id="copy_deduplicate",
        external_task_id="telemetry_derived__core_clients_first_seen__v1",
        execution_delta=datetime.timedelta(seconds=3600),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    accounts_backend_derived__events_stream__v1 = bigquery_etl_query(
        task_id="accounts_backend_derived__events_stream__v1",
        destination_table="events_stream_v1",
        dataset_id="accounts_backend_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jrediger@mozilla.com",
        email=[
            "ascholtz@mozilla.com",
            "jrediger@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wstuckey@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        arguments=[
            "--billing-project",
            "moz-fx-data-backfill-2",
            "--schema_update_option",
            "ALLOW_FIELD_ADDITION",
        ],
        task_group=task_group_accounts_backend,
    )

    accounts_cirrus_derived__baseline_clients_daily__v1 = bigquery_etl_query(
        task_id="accounts_cirrus_derived__baseline_clients_daily__v1",
        destination_table="baseline_clients_daily_v1",
        dataset_id="accounts_cirrus_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_accounts_cirrus,
    )

    accounts_cirrus_derived__baseline_clients_first_seen__v1 = bigquery_etl_query(
        task_id="accounts_cirrus_derived__baseline_clients_first_seen__v1",
        destination_table="baseline_clients_first_seen_v1",
        dataset_id="accounts_cirrus_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=True,
        parameters=["submission_date:DATE:{{ds}}"],
        task_group=task_group_accounts_cirrus,
    )

    accounts_cirrus_derived__baseline_clients_last_seen__v1 = bigquery_etl_query(
        task_id="accounts_cirrus_derived__baseline_clients_last_seen__v1",
        destination_table="baseline_clients_last_seen_v1",
        dataset_id="accounts_cirrus_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=True,
        task_group=task_group_accounts_cirrus,
    )

    accounts_cirrus_derived__events_stream__v1 = bigquery_etl_query(
        task_id="accounts_cirrus_derived__events_stream__v1",
        destination_table="events_stream_v1",
        dataset_id="accounts_cirrus_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jrediger@mozilla.com",
        email=[
            "ascholtz@mozilla.com",
            "jrediger@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wstuckey@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        arguments=["--billing-project", "moz-fx-data-backfill-2"],
        task_group=task_group_accounts_cirrus,
    )

    accounts_cirrus_derived__metrics_clients_daily__v1 = bigquery_etl_query(
        task_id="accounts_cirrus_derived__metrics_clients_daily__v1",
        destination_table="metrics_clients_daily_v1",
        dataset_id="accounts_cirrus_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_accounts_cirrus,
    )

    accounts_frontend_derived__events_stream__v1 = bigquery_etl_query(
        task_id="accounts_frontend_derived__events_stream__v1",
        destination_table="events_stream_v1",
        dataset_id="accounts_frontend_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jrediger@mozilla.com",
        email=[
            "ascholtz@mozilla.com",
            "jrediger@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wstuckey@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        arguments=[
            "--billing-project",
            "moz-fx-data-backfill-2",
            "--schema_update_option",
            "ALLOW_FIELD_ADDITION",
        ],
        task_group=task_group_accounts_frontend,
    )

    ads_backend_derived__events_stream__v1 = bigquery_etl_query(
        task_id="ads_backend_derived__events_stream__v1",
        destination_table="events_stream_v1",
        dataset_id="ads_backend_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jrediger@mozilla.com",
        email=[
            "ascholtz@mozilla.com",
            "jrediger@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wstuckey@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        arguments=["--billing-project", "moz-fx-data-backfill-2"],
        task_group=task_group_ads_backend,
    )

    bedrock_derived__events_stream__v1 = bigquery_etl_query(
        task_id="bedrock_derived__events_stream__v1",
        destination_table="events_stream_v1",
        dataset_id="bedrock_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jrediger@mozilla.com",
        email=[
            "ascholtz@mozilla.com",
            "jrediger@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wstuckey@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        arguments=["--billing-project", "moz-fx-data-backfill-2"],
        task_group=task_group_bedrock,
    )

    bigeye__fenix_derived__metrics_clients_daily__v1 = bigquery_bigeye_check(
        task_id="bigeye__fenix_derived__metrics_clients_daily__v1",
        table_id="moz-fx-data-shared-prod.fenix_derived.metrics_clients_daily_v1",
        warehouse_id="1939",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
        task_group=task_group_fenix,
    )

    bigeye__fenix_derived__metrics_clients_last_seen__v1 = bigquery_bigeye_check(
        task_id="bigeye__fenix_derived__metrics_clients_last_seen__v1",
        table_id="moz-fx-data-shared-prod.fenix_derived.metrics_clients_last_seen_v1",
        warehouse_id="1939",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
        task_group=task_group_fenix,
    )

    bigeye__firefox_desktop_background_defaultagent_derived__baseline_clients_daily__v1 = bigquery_bigeye_check(
        task_id="bigeye__firefox_desktop_background_defaultagent_derived__baseline_clients_daily__v1",
        table_id="moz-fx-data-shared-prod.firefox_desktop_background_defaultagent_derived.baseline_clients_daily_v1",
        warehouse_id="1939",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
        task_group=task_group_firefox_desktop_background_defaultagent,
    )

    bigeye__firefox_desktop_background_defaultagent_derived__baseline_clients_first_seen__v1 = bigquery_bigeye_check(
        task_id="bigeye__firefox_desktop_background_defaultagent_derived__baseline_clients_first_seen__v1",
        table_id="moz-fx-data-shared-prod.firefox_desktop_background_defaultagent_derived.baseline_clients_first_seen_v1",
        warehouse_id="1939",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
        task_group=task_group_firefox_desktop_background_defaultagent,
    )

    bigeye__firefox_desktop_background_defaultagent_derived__baseline_clients_last_seen__v1 = bigquery_bigeye_check(
        task_id="bigeye__firefox_desktop_background_defaultagent_derived__baseline_clients_last_seen__v1",
        table_id="moz-fx-data-shared-prod.firefox_desktop_background_defaultagent_derived.baseline_clients_last_seen_v1",
        warehouse_id="1939",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
        task_group=task_group_firefox_desktop_background_defaultagent,
    )

    bigeye__firefox_desktop_background_defaultagent_derived__metrics_clients_daily__v1 = bigquery_bigeye_check(
        task_id="bigeye__firefox_desktop_background_defaultagent_derived__metrics_clients_daily__v1",
        table_id="moz-fx-data-shared-prod.firefox_desktop_background_defaultagent_derived.metrics_clients_daily_v1",
        warehouse_id="1939",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
        task_group=task_group_firefox_desktop_background_defaultagent,
    )

    bigeye__firefox_desktop_background_tasks_derived__baseline_clients_daily__v1 = bigquery_bigeye_check(
        task_id="bigeye__firefox_desktop_background_tasks_derived__baseline_clients_daily__v1",
        table_id="moz-fx-data-shared-prod.firefox_desktop_background_tasks_derived.baseline_clients_daily_v1",
        warehouse_id="1939",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
        task_group=task_group_firefox_desktop_background_tasks,
    )

    bigeye__firefox_desktop_background_tasks_derived__baseline_clients_first_seen__v1 = bigquery_bigeye_check(
        task_id="bigeye__firefox_desktop_background_tasks_derived__baseline_clients_first_seen__v1",
        table_id="moz-fx-data-shared-prod.firefox_desktop_background_tasks_derived.baseline_clients_first_seen_v1",
        warehouse_id="1939",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
        task_group=task_group_firefox_desktop_background_tasks,
    )

    bigeye__firefox_desktop_background_tasks_derived__baseline_clients_last_seen__v1 = bigquery_bigeye_check(
        task_id="bigeye__firefox_desktop_background_tasks_derived__baseline_clients_last_seen__v1",
        table_id="moz-fx-data-shared-prod.firefox_desktop_background_tasks_derived.baseline_clients_last_seen_v1",
        warehouse_id="1939",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
        task_group=task_group_firefox_desktop_background_tasks,
    )

    bigeye__firefox_desktop_background_tasks_derived__metrics_clients_daily__v1 = bigquery_bigeye_check(
        task_id="bigeye__firefox_desktop_background_tasks_derived__metrics_clients_daily__v1",
        table_id="moz-fx-data-shared-prod.firefox_desktop_background_tasks_derived.metrics_clients_daily_v1",
        warehouse_id="1939",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
        task_group=task_group_firefox_desktop_background_tasks,
    )

    bigeye__firefox_desktop_background_update_derived__baseline_clients_daily__v1 = bigquery_bigeye_check(
        task_id="bigeye__firefox_desktop_background_update_derived__baseline_clients_daily__v1",
        table_id="moz-fx-data-shared-prod.firefox_desktop_background_update_derived.baseline_clients_daily_v1",
        warehouse_id="1939",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
        task_group=task_group_firefox_desktop_background_update,
    )

    bigeye__firefox_desktop_background_update_derived__baseline_clients_first_seen__v1 = bigquery_bigeye_check(
        task_id="bigeye__firefox_desktop_background_update_derived__baseline_clients_first_seen__v1",
        table_id="moz-fx-data-shared-prod.firefox_desktop_background_update_derived.baseline_clients_first_seen_v1",
        warehouse_id="1939",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
        task_group=task_group_firefox_desktop_background_update,
    )

    bigeye__firefox_desktop_background_update_derived__baseline_clients_last_seen__v1 = bigquery_bigeye_check(
        task_id="bigeye__firefox_desktop_background_update_derived__baseline_clients_last_seen__v1",
        table_id="moz-fx-data-shared-prod.firefox_desktop_background_update_derived.baseline_clients_last_seen_v1",
        warehouse_id="1939",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
        task_group=task_group_firefox_desktop_background_update,
    )

    bigeye__firefox_desktop_background_update_derived__metrics_clients_daily__v1 = bigquery_bigeye_check(
        task_id="bigeye__firefox_desktop_background_update_derived__metrics_clients_daily__v1",
        table_id="moz-fx-data-shared-prod.firefox_desktop_background_update_derived.metrics_clients_daily_v1",
        warehouse_id="1939",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
        task_group=task_group_firefox_desktop_background_update,
    )

    bigeye__firefox_desktop_background_update_derived__metrics_clients_last_seen__v1 = bigquery_bigeye_check(
        task_id="bigeye__firefox_desktop_background_update_derived__metrics_clients_last_seen__v1",
        table_id="moz-fx-data-shared-prod.firefox_desktop_background_update_derived.metrics_clients_last_seen_v1",
        warehouse_id="1939",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
        task_group=task_group_firefox_desktop_background_update,
    )

    bigeye__firefox_desktop_derived__baseline_clients_daily__v1 = bigquery_bigeye_check(
        task_id="bigeye__firefox_desktop_derived__baseline_clients_daily__v1",
        table_id="moz-fx-data-shared-prod.firefox_desktop_derived.baseline_clients_daily_v1",
        warehouse_id="1939",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
        task_group=task_group_firefox_desktop,
    )

    bigeye__firefox_desktop_derived__baseline_clients_first_seen__v1 = bigquery_bigeye_check(
        task_id="bigeye__firefox_desktop_derived__baseline_clients_first_seen__v1",
        table_id="moz-fx-data-shared-prod.firefox_desktop_derived.baseline_clients_first_seen_v1",
        warehouse_id="1939",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
        task_group=task_group_firefox_desktop,
    )

    bigeye__firefox_desktop_derived__baseline_clients_last_seen__v1 = bigquery_bigeye_check(
        task_id="bigeye__firefox_desktop_derived__baseline_clients_last_seen__v1",
        table_id="moz-fx-data-shared-prod.firefox_desktop_derived.baseline_clients_last_seen_v1",
        warehouse_id="1939",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
        task_group=task_group_firefox_desktop,
    )

    bigeye__firefox_desktop_derived__metrics_clients_daily__v1 = bigquery_bigeye_check(
        task_id="bigeye__firefox_desktop_derived__metrics_clients_daily__v1",
        table_id="moz-fx-data-shared-prod.firefox_desktop_derived.metrics_clients_daily_v1",
        warehouse_id="1939",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
        task_group=task_group_firefox_desktop,
    )

    bigeye__firefox_desktop_derived__metrics_clients_last_seen__v1 = bigquery_bigeye_check(
        task_id="bigeye__firefox_desktop_derived__metrics_clients_last_seen__v1",
        table_id="moz-fx-data-shared-prod.firefox_desktop_derived.metrics_clients_last_seen_v1",
        warehouse_id="1939",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
        task_group=task_group_firefox_desktop,
    )

    bigeye__firefox_ios_derived__metrics_clients_daily__v1 = bigquery_bigeye_check(
        task_id="bigeye__firefox_ios_derived__metrics_clients_daily__v1",
        table_id="moz-fx-data-shared-prod.firefox_ios_derived.metrics_clients_daily_v1",
        warehouse_id="1939",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
        task_group=task_group_firefox_ios,
    )

    bigeye__firefox_ios_derived__metrics_clients_last_seen__v1 = bigquery_bigeye_check(
        task_id="bigeye__firefox_ios_derived__metrics_clients_last_seen__v1",
        table_id="moz-fx-data-shared-prod.firefox_ios_derived.metrics_clients_last_seen_v1",
        warehouse_id="1939",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
        task_group=task_group_firefox_ios,
    )

    bigeye__org_mozilla_fenix_derived__baseline_clients_daily__v1 = bigquery_bigeye_check(
        task_id="bigeye__org_mozilla_fenix_derived__baseline_clients_daily__v1",
        table_id="moz-fx-data-shared-prod.org_mozilla_fenix_derived.baseline_clients_daily_v1",
        warehouse_id="1939",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
        task_group=task_group_fenix,
    )

    bigeye__org_mozilla_fenix_derived__baseline_clients_first_seen__v1 = bigquery_bigeye_check(
        task_id="bigeye__org_mozilla_fenix_derived__baseline_clients_first_seen__v1",
        table_id="moz-fx-data-shared-prod.org_mozilla_fenix_derived.baseline_clients_first_seen_v1",
        warehouse_id="1939",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
        task_group=task_group_fenix,
    )

    bigeye__org_mozilla_fenix_derived__baseline_clients_last_seen__v1 = bigquery_bigeye_check(
        task_id="bigeye__org_mozilla_fenix_derived__baseline_clients_last_seen__v1",
        table_id="moz-fx-data-shared-prod.org_mozilla_fenix_derived.baseline_clients_last_seen_v1",
        warehouse_id="1939",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
        task_group=task_group_fenix,
    )

    bigeye__org_mozilla_fenix_nightly_derived__baseline_clients_daily__v1 = bigquery_bigeye_check(
        task_id="bigeye__org_mozilla_fenix_nightly_derived__baseline_clients_daily__v1",
        table_id="moz-fx-data-shared-prod.org_mozilla_fenix_nightly_derived.baseline_clients_daily_v1",
        warehouse_id="1939",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
        task_group=task_group_fenix,
    )

    bigeye__org_mozilla_fenix_nightly_derived__baseline_clients_first_seen__v1 = bigquery_bigeye_check(
        task_id="bigeye__org_mozilla_fenix_nightly_derived__baseline_clients_first_seen__v1",
        table_id="moz-fx-data-shared-prod.org_mozilla_fenix_nightly_derived.baseline_clients_first_seen_v1",
        warehouse_id="1939",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
        task_group=task_group_fenix,
    )

    bigeye__org_mozilla_fenix_nightly_derived__baseline_clients_last_seen__v1 = bigquery_bigeye_check(
        task_id="bigeye__org_mozilla_fenix_nightly_derived__baseline_clients_last_seen__v1",
        table_id="moz-fx-data-shared-prod.org_mozilla_fenix_nightly_derived.baseline_clients_last_seen_v1",
        warehouse_id="1939",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
        task_group=task_group_fenix,
    )

    bigeye__org_mozilla_fennec_aurora_derived__baseline_clients_daily__v1 = bigquery_bigeye_check(
        task_id="bigeye__org_mozilla_fennec_aurora_derived__baseline_clients_daily__v1",
        table_id="moz-fx-data-shared-prod.org_mozilla_fennec_aurora_derived.baseline_clients_daily_v1",
        warehouse_id="1939",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
        task_group=task_group_fenix,
    )

    bigeye__org_mozilla_fennec_aurora_derived__baseline_clients_first_seen__v1 = bigquery_bigeye_check(
        task_id="bigeye__org_mozilla_fennec_aurora_derived__baseline_clients_first_seen__v1",
        table_id="moz-fx-data-shared-prod.org_mozilla_fennec_aurora_derived.baseline_clients_first_seen_v1",
        warehouse_id="1939",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
        task_group=task_group_fenix,
    )

    bigeye__org_mozilla_fennec_aurora_derived__baseline_clients_last_seen__v1 = bigquery_bigeye_check(
        task_id="bigeye__org_mozilla_fennec_aurora_derived__baseline_clients_last_seen__v1",
        table_id="moz-fx-data-shared-prod.org_mozilla_fennec_aurora_derived.baseline_clients_last_seen_v1",
        warehouse_id="1939",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
        task_group=task_group_fenix,
    )

    bigeye__org_mozilla_firefox_beta_derived__baseline_clients_daily__v1 = bigquery_bigeye_check(
        task_id="bigeye__org_mozilla_firefox_beta_derived__baseline_clients_daily__v1",
        table_id="moz-fx-data-shared-prod.org_mozilla_firefox_beta_derived.baseline_clients_daily_v1",
        warehouse_id="1939",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
        task_group=task_group_fenix,
    )

    bigeye__org_mozilla_firefox_beta_derived__baseline_clients_first_seen__v1 = bigquery_bigeye_check(
        task_id="bigeye__org_mozilla_firefox_beta_derived__baseline_clients_first_seen__v1",
        table_id="moz-fx-data-shared-prod.org_mozilla_firefox_beta_derived.baseline_clients_first_seen_v1",
        warehouse_id="1939",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
        task_group=task_group_fenix,
    )

    bigeye__org_mozilla_firefox_beta_derived__baseline_clients_last_seen__v1 = bigquery_bigeye_check(
        task_id="bigeye__org_mozilla_firefox_beta_derived__baseline_clients_last_seen__v1",
        table_id="moz-fx-data-shared-prod.org_mozilla_firefox_beta_derived.baseline_clients_last_seen_v1",
        warehouse_id="1939",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
        task_group=task_group_fenix,
    )

    bigeye__org_mozilla_firefox_derived__baseline_clients_daily__v1 = bigquery_bigeye_check(
        task_id="bigeye__org_mozilla_firefox_derived__baseline_clients_daily__v1",
        table_id="moz-fx-data-shared-prod.org_mozilla_firefox_derived.baseline_clients_daily_v1",
        warehouse_id="1939",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
        task_group=task_group_fenix,
    )

    bigeye__org_mozilla_firefox_derived__baseline_clients_first_seen__v1 = bigquery_bigeye_check(
        task_id="bigeye__org_mozilla_firefox_derived__baseline_clients_first_seen__v1",
        table_id="moz-fx-data-shared-prod.org_mozilla_firefox_derived.baseline_clients_first_seen_v1",
        warehouse_id="1939",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
        task_group=task_group_fenix,
    )

    bigeye__org_mozilla_firefox_derived__baseline_clients_last_seen__v1 = bigquery_bigeye_check(
        task_id="bigeye__org_mozilla_firefox_derived__baseline_clients_last_seen__v1",
        table_id="moz-fx-data-shared-prod.org_mozilla_firefox_derived.baseline_clients_last_seen_v1",
        warehouse_id="1939",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
        task_group=task_group_fenix,
    )

    bigeye__org_mozilla_ios_fennec_derived__baseline_clients_daily__v1 = bigquery_bigeye_check(
        task_id="bigeye__org_mozilla_ios_fennec_derived__baseline_clients_daily__v1",
        table_id="moz-fx-data-shared-prod.org_mozilla_ios_fennec_derived.baseline_clients_daily_v1",
        warehouse_id="1939",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
        task_group=task_group_firefox_ios,
    )

    bigeye__org_mozilla_ios_fennec_derived__baseline_clients_first_seen__v1 = bigquery_bigeye_check(
        task_id="bigeye__org_mozilla_ios_fennec_derived__baseline_clients_first_seen__v1",
        table_id="moz-fx-data-shared-prod.org_mozilla_ios_fennec_derived.baseline_clients_first_seen_v1",
        warehouse_id="1939",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
        task_group=task_group_firefox_ios,
    )

    bigeye__org_mozilla_ios_fennec_derived__baseline_clients_last_seen__v1 = bigquery_bigeye_check(
        task_id="bigeye__org_mozilla_ios_fennec_derived__baseline_clients_last_seen__v1",
        table_id="moz-fx-data-shared-prod.org_mozilla_ios_fennec_derived.baseline_clients_last_seen_v1",
        warehouse_id="1939",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
        task_group=task_group_firefox_ios,
    )

    bigeye__org_mozilla_ios_firefox_derived__baseline_clients_daily__v1 = bigquery_bigeye_check(
        task_id="bigeye__org_mozilla_ios_firefox_derived__baseline_clients_daily__v1",
        table_id="moz-fx-data-shared-prod.org_mozilla_ios_firefox_derived.baseline_clients_daily_v1",
        warehouse_id="1939",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
        task_group=task_group_firefox_ios,
    )

    bigeye__org_mozilla_ios_firefox_derived__baseline_clients_first_seen__v1 = bigquery_bigeye_check(
        task_id="bigeye__org_mozilla_ios_firefox_derived__baseline_clients_first_seen__v1",
        table_id="moz-fx-data-shared-prod.org_mozilla_ios_firefox_derived.baseline_clients_first_seen_v1",
        warehouse_id="1939",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
        task_group=task_group_firefox_ios,
    )

    bigeye__org_mozilla_ios_firefox_derived__baseline_clients_last_seen__v1 = bigquery_bigeye_check(
        task_id="bigeye__org_mozilla_ios_firefox_derived__baseline_clients_last_seen__v1",
        table_id="moz-fx-data-shared-prod.org_mozilla_ios_firefox_derived.baseline_clients_last_seen_v1",
        warehouse_id="1939",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
        task_group=task_group_firefox_ios,
    )

    bigeye__org_mozilla_ios_firefoxbeta_derived__baseline_clients_daily__v1 = bigquery_bigeye_check(
        task_id="bigeye__org_mozilla_ios_firefoxbeta_derived__baseline_clients_daily__v1",
        table_id="moz-fx-data-shared-prod.org_mozilla_ios_firefoxbeta_derived.baseline_clients_daily_v1",
        warehouse_id="1939",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
        task_group=task_group_firefox_ios,
    )

    bigeye__org_mozilla_ios_firefoxbeta_derived__baseline_clients_first_seen__v1 = bigquery_bigeye_check(
        task_id="bigeye__org_mozilla_ios_firefoxbeta_derived__baseline_clients_first_seen__v1",
        table_id="moz-fx-data-shared-prod.org_mozilla_ios_firefoxbeta_derived.baseline_clients_first_seen_v1",
        warehouse_id="1939",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
        task_group=task_group_firefox_ios,
    )

    bigeye__org_mozilla_ios_firefoxbeta_derived__baseline_clients_last_seen__v1 = bigquery_bigeye_check(
        task_id="bigeye__org_mozilla_ios_firefoxbeta_derived__baseline_clients_last_seen__v1",
        table_id="moz-fx-data-shared-prod.org_mozilla_ios_firefoxbeta_derived.baseline_clients_last_seen_v1",
        warehouse_id="1939",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
        task_group=task_group_firefox_ios,
    )

    bigeye__org_mozilla_ios_focus_derived__baseline_clients_last_seen__v1 = bigquery_bigeye_check(
        task_id="bigeye__org_mozilla_ios_focus_derived__baseline_clients_last_seen__v1",
        table_id="moz-fx-data-shared-prod.org_mozilla_ios_focus_derived.baseline_clients_last_seen_v1",
        warehouse_id="1939",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
        task_group=task_group_focus_ios,
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

    burnham_derived__events_stream__v1 = bigquery_etl_query(
        task_id="burnham_derived__events_stream__v1",
        destination_table="events_stream_v1",
        dataset_id="burnham_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jrediger@mozilla.com",
        email=[
            "ascholtz@mozilla.com",
            "jrediger@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wstuckey@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        arguments=["--billing-project", "moz-fx-data-backfill-2"],
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

    checks__fail_firefox_desktop_background_defaultagent_derived__baseline_clients_last_seen__v1 = bigquery_dq_check(
        task_id="checks__fail_firefox_desktop_background_defaultagent_derived__baseline_clients_last_seen__v1",
        source_table="baseline_clients_last_seen_v1",
        dataset_id="firefox_desktop_background_defaultagent_derived",
        project_id="moz-fx-data-shared-prod",
        is_dq_check_fail=True,
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        parameters=["submission_date:DATE:{{ds}}"],
        retries=0,
        task_group=task_group_firefox_desktop_background_defaultagent,
    )

    checks__fail_firefox_desktop_background_tasks_derived__baseline_clients_last_seen__v1 = bigquery_dq_check(
        task_id="checks__fail_firefox_desktop_background_tasks_derived__baseline_clients_last_seen__v1",
        source_table="baseline_clients_last_seen_v1",
        dataset_id="firefox_desktop_background_tasks_derived",
        project_id="moz-fx-data-shared-prod",
        is_dq_check_fail=True,
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        parameters=["submission_date:DATE:{{ds}}"],
        retries=0,
        task_group=task_group_firefox_desktop_background_tasks,
    )

    checks__fail_firefox_desktop_background_update_derived__baseline_clients_last_seen__v1 = bigquery_dq_check(
        task_id="checks__fail_firefox_desktop_background_update_derived__baseline_clients_last_seen__v1",
        source_table="baseline_clients_last_seen_v1",
        dataset_id="firefox_desktop_background_update_derived",
        project_id="moz-fx-data-shared-prod",
        is_dq_check_fail=True,
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        parameters=["submission_date:DATE:{{ds}}"],
        retries=0,
        task_group=task_group_firefox_desktop_background_update,
    )

    checks__fail_firefox_desktop_derived__baseline_clients_last_seen__v1 = bigquery_dq_check(
        task_id="checks__fail_firefox_desktop_derived__baseline_clients_last_seen__v1",
        source_table="baseline_clients_last_seen_v1",
        dataset_id="firefox_desktop_derived",
        project_id="moz-fx-data-shared-prod",
        is_dq_check_fail=True,
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        parameters=["submission_date:DATE:{{ds}}"],
        retries=0,
        task_group=task_group_firefox_desktop,
    )

    checks__fail_net_thunderbird_android_beta_derived__baseline_clients_last_seen__v1 = bigquery_dq_check(
        task_id="checks__fail_net_thunderbird_android_beta_derived__baseline_clients_last_seen__v1",
        source_table="baseline_clients_last_seen_v1",
        dataset_id="net_thunderbird_android_beta_derived",
        project_id="moz-fx-data-shared-prod",
        is_dq_check_fail=True,
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        parameters=["submission_date:DATE:{{ds}}"],
        retries=0,
        task_group=task_group_thunderbird_android,
    )

    checks__fail_net_thunderbird_android_derived__baseline_clients_last_seen__v1 = bigquery_dq_check(
        task_id="checks__fail_net_thunderbird_android_derived__baseline_clients_last_seen__v1",
        source_table="baseline_clients_last_seen_v1",
        dataset_id="net_thunderbird_android_derived",
        project_id="moz-fx-data-shared-prod",
        is_dq_check_fail=True,
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        parameters=["submission_date:DATE:{{ds}}"],
        retries=0,
        task_group=task_group_thunderbird_android,
    )

    checks__fail_org_mozilla_connect_firefox_derived__baseline_clients_last_seen__v1 = bigquery_dq_check(
        task_id="checks__fail_org_mozilla_connect_firefox_derived__baseline_clients_last_seen__v1",
        source_table="baseline_clients_last_seen_v1",
        dataset_id="org_mozilla_connect_firefox_derived",
        project_id="moz-fx-data-shared-prod",
        is_dq_check_fail=True,
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        parameters=["submission_date:DATE:{{ds}}"],
        retries=0,
        task_group=task_group_firefox_echo_show,
    )

    checks__fail_org_mozilla_fenix_derived__baseline_clients_last_seen__v1 = bigquery_dq_check(
        task_id="checks__fail_org_mozilla_fenix_derived__baseline_clients_last_seen__v1",
        source_table="baseline_clients_last_seen_v1",
        dataset_id="org_mozilla_fenix_derived",
        project_id="moz-fx-data-shared-prod",
        is_dq_check_fail=True,
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        parameters=["submission_date:DATE:{{ds}}"],
        retries=0,
        task_group=task_group_fenix,
    )

    checks__fail_org_mozilla_fenix_nightly_derived__baseline_clients_last_seen__v1 = bigquery_dq_check(
        task_id="checks__fail_org_mozilla_fenix_nightly_derived__baseline_clients_last_seen__v1",
        source_table="baseline_clients_last_seen_v1",
        dataset_id="org_mozilla_fenix_nightly_derived",
        project_id="moz-fx-data-shared-prod",
        is_dq_check_fail=True,
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        parameters=["submission_date:DATE:{{ds}}"],
        retries=0,
        task_group=task_group_fenix,
    )

    checks__fail_org_mozilla_fennec_aurora_derived__baseline_clients_last_seen__v1 = bigquery_dq_check(
        task_id="checks__fail_org_mozilla_fennec_aurora_derived__baseline_clients_last_seen__v1",
        source_table="baseline_clients_last_seen_v1",
        dataset_id="org_mozilla_fennec_aurora_derived",
        project_id="moz-fx-data-shared-prod",
        is_dq_check_fail=True,
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        parameters=["submission_date:DATE:{{ds}}"],
        retries=0,
        task_group=task_group_fenix,
    )

    checks__fail_org_mozilla_firefox_beta_derived__baseline_clients_last_seen__v1 = bigquery_dq_check(
        task_id="checks__fail_org_mozilla_firefox_beta_derived__baseline_clients_last_seen__v1",
        source_table="baseline_clients_last_seen_v1",
        dataset_id="org_mozilla_firefox_beta_derived",
        project_id="moz-fx-data-shared-prod",
        is_dq_check_fail=True,
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        parameters=["submission_date:DATE:{{ds}}"],
        retries=0,
        task_group=task_group_fenix,
    )

    checks__fail_org_mozilla_firefox_derived__baseline_clients_last_seen__v1 = bigquery_dq_check(
        task_id="checks__fail_org_mozilla_firefox_derived__baseline_clients_last_seen__v1",
        source_table="baseline_clients_last_seen_v1",
        dataset_id="org_mozilla_firefox_derived",
        project_id="moz-fx-data-shared-prod",
        is_dq_check_fail=True,
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        parameters=["submission_date:DATE:{{ds}}"],
        retries=0,
        task_group=task_group_fenix,
    )

    checks__fail_org_mozilla_focus_beta_derived__baseline_clients_last_seen__v1 = bigquery_dq_check(
        task_id="checks__fail_org_mozilla_focus_beta_derived__baseline_clients_last_seen__v1",
        source_table="baseline_clients_last_seen_v1",
        dataset_id="org_mozilla_focus_beta_derived",
        project_id="moz-fx-data-shared-prod",
        is_dq_check_fail=True,
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        parameters=["submission_date:DATE:{{ds}}"],
        retries=0,
        task_group=task_group_focus_android,
    )

    checks__fail_org_mozilla_focus_derived__baseline_clients_last_seen__v1 = bigquery_dq_check(
        task_id="checks__fail_org_mozilla_focus_derived__baseline_clients_last_seen__v1",
        source_table="baseline_clients_last_seen_v1",
        dataset_id="org_mozilla_focus_derived",
        project_id="moz-fx-data-shared-prod",
        is_dq_check_fail=True,
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        parameters=["submission_date:DATE:{{ds}}"],
        retries=0,
        task_group=task_group_focus_android,
    )

    checks__fail_org_mozilla_focus_nightly_derived__baseline_clients_last_seen__v1 = bigquery_dq_check(
        task_id="checks__fail_org_mozilla_focus_nightly_derived__baseline_clients_last_seen__v1",
        source_table="baseline_clients_last_seen_v1",
        dataset_id="org_mozilla_focus_nightly_derived",
        project_id="moz-fx-data-shared-prod",
        is_dq_check_fail=True,
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        parameters=["submission_date:DATE:{{ds}}"],
        retries=0,
        task_group=task_group_focus_android,
    )

    checks__fail_org_mozilla_ios_fennec_derived__baseline_clients_last_seen__v1 = bigquery_dq_check(
        task_id="checks__fail_org_mozilla_ios_fennec_derived__baseline_clients_last_seen__v1",
        source_table="baseline_clients_last_seen_v1",
        dataset_id="org_mozilla_ios_fennec_derived",
        project_id="moz-fx-data-shared-prod",
        is_dq_check_fail=True,
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        parameters=["submission_date:DATE:{{ds}}"],
        retries=0,
        task_group=task_group_firefox_ios,
    )

    checks__fail_org_mozilla_ios_firefox_derived__baseline_clients_last_seen__v1 = bigquery_dq_check(
        task_id="checks__fail_org_mozilla_ios_firefox_derived__baseline_clients_last_seen__v1",
        source_table="baseline_clients_last_seen_v1",
        dataset_id="org_mozilla_ios_firefox_derived",
        project_id="moz-fx-data-shared-prod",
        is_dq_check_fail=True,
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        parameters=["submission_date:DATE:{{ds}}"],
        retries=0,
        task_group=task_group_firefox_ios,
    )

    checks__fail_org_mozilla_ios_firefoxbeta_derived__baseline_clients_last_seen__v1 = bigquery_dq_check(
        task_id="checks__fail_org_mozilla_ios_firefoxbeta_derived__baseline_clients_last_seen__v1",
        source_table="baseline_clients_last_seen_v1",
        dataset_id="org_mozilla_ios_firefoxbeta_derived",
        project_id="moz-fx-data-shared-prod",
        is_dq_check_fail=True,
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        parameters=["submission_date:DATE:{{ds}}"],
        retries=0,
        task_group=task_group_firefox_ios,
    )

    checks__fail_org_mozilla_ios_focus_derived__baseline_clients_last_seen__v1 = bigquery_dq_check(
        task_id="checks__fail_org_mozilla_ios_focus_derived__baseline_clients_last_seen__v1",
        source_table="baseline_clients_last_seen_v1",
        dataset_id="org_mozilla_ios_focus_derived",
        project_id="moz-fx-data-shared-prod",
        is_dq_check_fail=True,
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        parameters=["submission_date:DATE:{{ds}}"],
        retries=0,
        task_group=task_group_focus_ios,
    )

    checks__fail_org_mozilla_ios_klar_derived__baseline_clients_last_seen__v1 = bigquery_dq_check(
        task_id="checks__fail_org_mozilla_ios_klar_derived__baseline_clients_last_seen__v1",
        source_table="baseline_clients_last_seen_v1",
        dataset_id="org_mozilla_ios_klar_derived",
        project_id="moz-fx-data-shared-prod",
        is_dq_check_fail=True,
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        parameters=["submission_date:DATE:{{ds}}"],
        retries=0,
        task_group=task_group_klar_ios,
    )

    checks__fail_org_mozilla_ios_lockbox_derived__baseline_clients_last_seen__v1 = bigquery_dq_check(
        task_id="checks__fail_org_mozilla_ios_lockbox_derived__baseline_clients_last_seen__v1",
        source_table="baseline_clients_last_seen_v1",
        dataset_id="org_mozilla_ios_lockbox_derived",
        project_id="moz-fx-data-shared-prod",
        is_dq_check_fail=True,
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        parameters=["submission_date:DATE:{{ds}}"],
        retries=0,
        task_group=task_group_lockwise_ios,
    )

    checks__fail_org_mozilla_klar_derived__baseline_clients_last_seen__v1 = bigquery_dq_check(
        task_id="checks__fail_org_mozilla_klar_derived__baseline_clients_last_seen__v1",
        source_table="baseline_clients_last_seen_v1",
        dataset_id="org_mozilla_klar_derived",
        project_id="moz-fx-data-shared-prod",
        is_dq_check_fail=True,
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        parameters=["submission_date:DATE:{{ds}}"],
        retries=0,
        task_group=task_group_klar_android,
    )

    checks__fail_pine_derived__baseline_clients_last_seen__v1 = bigquery_dq_check(
        task_id="checks__fail_pine_derived__baseline_clients_last_seen__v1",
        source_table="baseline_clients_last_seen_v1",
        dataset_id="pine_derived",
        project_id="moz-fx-data-shared-prod",
        is_dq_check_fail=True,
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        parameters=["submission_date:DATE:{{ds}}"],
        retries=0,
        task_group=task_group_pine,
    )

    checks__fail_thunderbird_desktop_derived__baseline_clients_last_seen__v1 = bigquery_dq_check(
        task_id="checks__fail_thunderbird_desktop_derived__baseline_clients_last_seen__v1",
        source_table="baseline_clients_last_seen_v1",
        dataset_id="thunderbird_desktop_derived",
        project_id="moz-fx-data-shared-prod",
        is_dq_check_fail=True,
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        parameters=["submission_date:DATE:{{ds}}"],
        retries=0,
        task_group=task_group_thunderbird_desktop,
    )

    checks__warn_net_thunderbird_android_daily_derived__baseline_clients_last_seen__v1 = bigquery_dq_check(
        task_id="checks__warn_net_thunderbird_android_daily_derived__baseline_clients_last_seen__v1",
        source_table="baseline_clients_last_seen_v1",
        dataset_id="net_thunderbird_android_daily_derived",
        project_id="moz-fx-data-shared-prod",
        is_dq_check_fail=False,
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        parameters=["submission_date:DATE:{{ds}}"],
        retries=0,
        task_group=task_group_thunderbird_android,
    )

    checks__warn_net_thunderbird_android_derived__baseline_clients_last_seen__v1 = bigquery_dq_check(
        task_id="checks__warn_net_thunderbird_android_derived__baseline_clients_last_seen__v1",
        source_table="baseline_clients_last_seen_v1",
        dataset_id="net_thunderbird_android_derived",
        project_id="moz-fx-data-shared-prod",
        is_dq_check_fail=False,
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        parameters=["submission_date:DATE:{{ds}}"],
        retries=0,
        task_group=task_group_thunderbird_android,
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

    checks__warn_org_mozilla_social_nightly_derived__baseline_clients_last_seen__v1 = bigquery_dq_check(
        task_id="checks__warn_org_mozilla_social_nightly_derived__baseline_clients_last_seen__v1",
        source_table="baseline_clients_last_seen_v1",
        dataset_id="org_mozilla_social_nightly_derived",
        project_id="moz-fx-data-shared-prod",
        is_dq_check_fail=False,
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        parameters=["submission_date:DATE:{{ds}}"],
        retries=0,
        task_group=task_group_org_mozilla_social_nightly,
    )

    debug_ping_view_derived__events_stream__v1 = bigquery_etl_query(
        task_id="debug_ping_view_derived__events_stream__v1",
        destination_table="events_stream_v1",
        dataset_id="debug_ping_view_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jrediger@mozilla.com",
        email=[
            "ascholtz@mozilla.com",
            "jrediger@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wstuckey@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        arguments=["--billing-project", "moz-fx-data-backfill-2"],
        task_group=task_group_debug_ping_view,
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

    firefox_crashreporter_derived__baseline_clients_daily__v1 = bigquery_etl_query(
        task_id="firefox_crashreporter_derived__baseline_clients_daily__v1",
        destination_table="baseline_clients_daily_v1",
        dataset_id="firefox_crashreporter_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_firefox_crashreporter,
    )

    firefox_crashreporter_derived__baseline_clients_first_seen__v1 = bigquery_etl_query(
        task_id="firefox_crashreporter_derived__baseline_clients_first_seen__v1",
        destination_table="baseline_clients_first_seen_v1",
        dataset_id="firefox_crashreporter_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=True,
        parameters=["submission_date:DATE:{{ds}}"],
        task_group=task_group_firefox_crashreporter,
    )

    firefox_crashreporter_derived__baseline_clients_last_seen__v1 = bigquery_etl_query(
        task_id="firefox_crashreporter_derived__baseline_clients_last_seen__v1",
        destination_table="baseline_clients_last_seen_v1",
        dataset_id="firefox_crashreporter_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=True,
        task_group=task_group_firefox_crashreporter,
    )

    firefox_crashreporter_derived__events_stream__v1 = bigquery_etl_query(
        task_id="firefox_crashreporter_derived__events_stream__v1",
        destination_table="events_stream_v1",
        dataset_id="firefox_crashreporter_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jrediger@mozilla.com",
        email=[
            "ascholtz@mozilla.com",
            "jrediger@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wstuckey@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        arguments=["--billing-project", "moz-fx-data-backfill-2"],
        task_group=task_group_firefox_crashreporter,
    )

    firefox_crashreporter_derived__metrics_clients_daily__v1 = bigquery_etl_query(
        task_id="firefox_crashreporter_derived__metrics_clients_daily__v1",
        destination_table="metrics_clients_daily_v1",
        dataset_id="firefox_crashreporter_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_firefox_crashreporter,
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

    firefox_desktop_background_defaultagent_derived__events_stream__v1 = bigquery_etl_query(
        task_id="firefox_desktop_background_defaultagent_derived__events_stream__v1",
        destination_table="events_stream_v1",
        dataset_id="firefox_desktop_background_defaultagent_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jrediger@mozilla.com",
        email=[
            "ascholtz@mozilla.com",
            "jrediger@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wstuckey@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        arguments=["--billing-project", "moz-fx-data-backfill-2"],
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

    firefox_desktop_background_tasks_derived__events_stream__v1 = bigquery_etl_query(
        task_id="firefox_desktop_background_tasks_derived__events_stream__v1",
        destination_table="events_stream_v1",
        dataset_id="firefox_desktop_background_tasks_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jrediger@mozilla.com",
        email=[
            "ascholtz@mozilla.com",
            "jrediger@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wstuckey@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        arguments=["--billing-project", "moz-fx-data-backfill-2"],
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

    firefox_desktop_background_update_derived__events_stream__v1 = bigquery_etl_query(
        task_id="firefox_desktop_background_update_derived__events_stream__v1",
        destination_table="events_stream_v1",
        dataset_id="firefox_desktop_background_update_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jrediger@mozilla.com",
        email=[
            "ascholtz@mozilla.com",
            "jrediger@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wstuckey@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        arguments=["--billing-project", "moz-fx-data-backfill-2"],
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

    firefox_desktop_derived__events_stream__v1 = bigquery_etl_query(
        task_id="firefox_desktop_derived__events_stream__v1",
        destination_table="events_stream_v1",
        dataset_id="firefox_desktop_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jrediger@mozilla.com",
        email=[
            "ascholtz@mozilla.com",
            "jrediger@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wstuckey@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        arguments=["--billing-project", "moz-fx-data-backfill-2"],
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

    firefox_translations_derived__events_stream__v1 = bigquery_etl_query(
        task_id="firefox_translations_derived__events_stream__v1",
        destination_table="events_stream_v1",
        dataset_id="firefox_translations_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jrediger@mozilla.com",
        email=[
            "ascholtz@mozilla.com",
            "jrediger@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wstuckey@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        arguments=["--billing-project", "moz-fx-data-backfill-2"],
        task_group=task_group_firefox_translations,
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

    glean_dictionary_derived__events_stream__v1 = bigquery_etl_query(
        task_id="glean_dictionary_derived__events_stream__v1",
        destination_table="events_stream_v1",
        dataset_id="glean_dictionary_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jrediger@mozilla.com",
        email=[
            "ascholtz@mozilla.com",
            "jrediger@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wstuckey@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        arguments=["--billing-project", "moz-fx-data-backfill-2"],
        task_group=task_group_glean_dictionary,
    )

    gleanjs_docs_derived__events_stream__v1 = bigquery_etl_query(
        task_id="gleanjs_docs_derived__events_stream__v1",
        destination_table="events_stream_v1",
        dataset_id="gleanjs_docs_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jrediger@mozilla.com",
        email=[
            "ascholtz@mozilla.com",
            "jrediger@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wstuckey@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        arguments=["--billing-project", "moz-fx-data-backfill-2"],
        task_group=task_group_gleanjs_docs,
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

    mdn_yari_derived__events_stream__v1 = bigquery_etl_query(
        task_id="mdn_yari_derived__events_stream__v1",
        destination_table="events_stream_v1",
        dataset_id="mdn_yari_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jrediger@mozilla.com",
        email=[
            "ascholtz@mozilla.com",
            "jrediger@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wstuckey@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        arguments=["--billing-project", "moz-fx-data-backfill-2"],
        task_group=task_group_mdn_yari,
    )

    monitor_backend_derived__events_stream__v1 = bigquery_etl_query(
        task_id="monitor_backend_derived__events_stream__v1",
        destination_table="events_stream_v1",
        dataset_id="monitor_backend_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jrediger@mozilla.com",
        email=[
            "ascholtz@mozilla.com",
            "jrediger@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wstuckey@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        arguments=["--billing-project", "moz-fx-data-backfill-2"],
        task_group=task_group_monitor_backend,
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

    monitor_cirrus_derived__events_stream__v1 = bigquery_etl_query(
        task_id="monitor_cirrus_derived__events_stream__v1",
        destination_table="events_stream_v1",
        dataset_id="monitor_cirrus_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jrediger@mozilla.com",
        email=[
            "ascholtz@mozilla.com",
            "jrediger@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wstuckey@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        arguments=["--billing-project", "moz-fx-data-backfill-2"],
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

    monitor_frontend_derived__events_stream__v1 = bigquery_etl_query(
        task_id="monitor_frontend_derived__events_stream__v1",
        destination_table="events_stream_v1",
        dataset_id="monitor_frontend_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jrediger@mozilla.com",
        email=[
            "ascholtz@mozilla.com",
            "jrediger@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wstuckey@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        arguments=["--billing-project", "moz-fx-data-backfill-2"],
        task_group=task_group_monitor_frontend,
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

    mozilla_lockbox_derived__events_stream__v1 = bigquery_etl_query(
        task_id="mozilla_lockbox_derived__events_stream__v1",
        destination_table="events_stream_v1",
        dataset_id="mozilla_lockbox_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jrediger@mozilla.com",
        email=[
            "ascholtz@mozilla.com",
            "jrediger@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wstuckey@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        arguments=["--billing-project", "moz-fx-data-backfill-2"],
        task_group=task_group_lockwise_android,
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

    mozilla_mach_derived__events_stream__v1 = bigquery_etl_query(
        task_id="mozilla_mach_derived__events_stream__v1",
        destination_table="events_stream_v1",
        dataset_id="mozilla_mach_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jrediger@mozilla.com",
        email=[
            "ascholtz@mozilla.com",
            "jrediger@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wstuckey@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        arguments=["--billing-project", "moz-fx-data-backfill-2"],
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

    mozillavpn_backend_cirrus_derived__baseline_clients_daily__v1 = bigquery_etl_query(
        task_id="mozillavpn_backend_cirrus_derived__baseline_clients_daily__v1",
        destination_table="baseline_clients_daily_v1",
        dataset_id="mozillavpn_backend_cirrus_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_mozillavpn_backend_cirrus,
    )

    mozillavpn_backend_cirrus_derived__baseline_clients_first_seen__v1 = bigquery_etl_query(
        task_id="mozillavpn_backend_cirrus_derived__baseline_clients_first_seen__v1",
        destination_table="baseline_clients_first_seen_v1",
        dataset_id="mozillavpn_backend_cirrus_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=True,
        parameters=["submission_date:DATE:{{ds}}"],
        task_group=task_group_mozillavpn_backend_cirrus,
    )

    mozillavpn_backend_cirrus_derived__baseline_clients_last_seen__v1 = (
        bigquery_etl_query(
            task_id="mozillavpn_backend_cirrus_derived__baseline_clients_last_seen__v1",
            destination_table="baseline_clients_last_seen_v1",
            dataset_id="mozillavpn_backend_cirrus_derived",
            project_id="moz-fx-data-shared-prod",
            owner="ascholtz@mozilla.com",
            email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
            date_partition_parameter="submission_date",
            depends_on_past=True,
            task_group=task_group_mozillavpn_backend_cirrus,
        )
    )

    mozillavpn_backend_cirrus_derived__events_stream__v1 = bigquery_etl_query(
        task_id="mozillavpn_backend_cirrus_derived__events_stream__v1",
        destination_table="events_stream_v1",
        dataset_id="mozillavpn_backend_cirrus_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jrediger@mozilla.com",
        email=[
            "ascholtz@mozilla.com",
            "jrediger@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wstuckey@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        arguments=["--billing-project", "moz-fx-data-backfill-2"],
        task_group=task_group_mozillavpn_backend_cirrus,
    )

    mozillavpn_backend_cirrus_derived__metrics_clients_daily__v1 = bigquery_etl_query(
        task_id="mozillavpn_backend_cirrus_derived__metrics_clients_daily__v1",
        destination_table="metrics_clients_daily_v1",
        dataset_id="mozillavpn_backend_cirrus_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_mozillavpn_backend_cirrus,
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

    mozillavpn_derived__events_stream__v1 = bigquery_etl_query(
        task_id="mozillavpn_derived__events_stream__v1",
        destination_table="events_stream_v1",
        dataset_id="mozillavpn_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jrediger@mozilla.com",
        email=[
            "ascholtz@mozilla.com",
            "jrediger@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wstuckey@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        arguments=["--billing-project", "moz-fx-data-backfill-2"],
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

    mozphab_derived__events_stream__v1 = bigquery_etl_query(
        task_id="mozphab_derived__events_stream__v1",
        destination_table="events_stream_v1",
        dataset_id="mozphab_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jrediger@mozilla.com",
        email=[
            "ascholtz@mozilla.com",
            "jrediger@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wstuckey@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        arguments=["--billing-project", "moz-fx-data-backfill-2"],
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

    net_thunderbird_android_beta_derived__baseline_clients_daily__v1 = (
        bigquery_etl_query(
            task_id="net_thunderbird_android_beta_derived__baseline_clients_daily__v1",
            destination_table="baseline_clients_daily_v1",
            dataset_id="net_thunderbird_android_beta_derived",
            project_id="moz-fx-data-shared-prod",
            owner="ascholtz@mozilla.com",
            email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
            date_partition_parameter="submission_date",
            depends_on_past=False,
            task_group=task_group_thunderbird_android,
        )
    )

    net_thunderbird_android_beta_derived__baseline_clients_first_seen__v1 = bigquery_etl_query(
        task_id="net_thunderbird_android_beta_derived__baseline_clients_first_seen__v1",
        destination_table="baseline_clients_first_seen_v1",
        dataset_id="net_thunderbird_android_beta_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=True,
        parameters=["submission_date:DATE:{{ds}}"],
        task_group=task_group_thunderbird_android,
    )

    net_thunderbird_android_beta_derived__baseline_clients_last_seen__v1 = bigquery_etl_query(
        task_id="net_thunderbird_android_beta_derived__baseline_clients_last_seen__v1",
        destination_table="baseline_clients_last_seen_v1",
        dataset_id="net_thunderbird_android_beta_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=True,
        task_group=task_group_thunderbird_android,
    )

    net_thunderbird_android_beta_derived__events_stream__v1 = bigquery_etl_query(
        task_id="net_thunderbird_android_beta_derived__events_stream__v1",
        destination_table="events_stream_v1",
        dataset_id="net_thunderbird_android_beta_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jrediger@mozilla.com",
        email=[
            "ascholtz@mozilla.com",
            "jrediger@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wstuckey@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        arguments=["--billing-project", "moz-fx-data-backfill-2"],
        task_group=task_group_thunderbird_android,
    )

    net_thunderbird_android_daily_derived__baseline_clients_daily__v1 = (
        bigquery_etl_query(
            task_id="net_thunderbird_android_daily_derived__baseline_clients_daily__v1",
            destination_table="baseline_clients_daily_v1",
            dataset_id="net_thunderbird_android_daily_derived",
            project_id="moz-fx-data-shared-prod",
            owner="ascholtz@mozilla.com",
            email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
            date_partition_parameter="submission_date",
            depends_on_past=False,
            task_group=task_group_thunderbird_android,
        )
    )

    net_thunderbird_android_daily_derived__baseline_clients_first_seen__v1 = bigquery_etl_query(
        task_id="net_thunderbird_android_daily_derived__baseline_clients_first_seen__v1",
        destination_table="baseline_clients_first_seen_v1",
        dataset_id="net_thunderbird_android_daily_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=True,
        parameters=["submission_date:DATE:{{ds}}"],
        task_group=task_group_thunderbird_android,
    )

    net_thunderbird_android_daily_derived__baseline_clients_last_seen__v1 = bigquery_etl_query(
        task_id="net_thunderbird_android_daily_derived__baseline_clients_last_seen__v1",
        destination_table="baseline_clients_last_seen_v1",
        dataset_id="net_thunderbird_android_daily_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=True,
        task_group=task_group_thunderbird_android,
    )

    net_thunderbird_android_daily_derived__events_stream__v1 = bigquery_etl_query(
        task_id="net_thunderbird_android_daily_derived__events_stream__v1",
        destination_table="events_stream_v1",
        dataset_id="net_thunderbird_android_daily_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jrediger@mozilla.com",
        email=[
            "ascholtz@mozilla.com",
            "jrediger@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wstuckey@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        arguments=["--billing-project", "moz-fx-data-backfill-2"],
        task_group=task_group_thunderbird_android,
    )

    net_thunderbird_android_derived__baseline_clients_daily__v1 = bigquery_etl_query(
        task_id="net_thunderbird_android_derived__baseline_clients_daily__v1",
        destination_table="baseline_clients_daily_v1",
        dataset_id="net_thunderbird_android_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_thunderbird_android,
    )

    net_thunderbird_android_derived__baseline_clients_first_seen__v1 = (
        bigquery_etl_query(
            task_id="net_thunderbird_android_derived__baseline_clients_first_seen__v1",
            destination_table="baseline_clients_first_seen_v1",
            dataset_id="net_thunderbird_android_derived",
            project_id="moz-fx-data-shared-prod",
            owner="ascholtz@mozilla.com",
            email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
            date_partition_parameter=None,
            depends_on_past=True,
            parameters=["submission_date:DATE:{{ds}}"],
            task_group=task_group_thunderbird_android,
        )
    )

    net_thunderbird_android_derived__baseline_clients_last_seen__v1 = (
        bigquery_etl_query(
            task_id="net_thunderbird_android_derived__baseline_clients_last_seen__v1",
            destination_table="baseline_clients_last_seen_v1",
            dataset_id="net_thunderbird_android_derived",
            project_id="moz-fx-data-shared-prod",
            owner="ascholtz@mozilla.com",
            email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
            date_partition_parameter="submission_date",
            depends_on_past=True,
            task_group=task_group_thunderbird_android,
        )
    )

    net_thunderbird_android_derived__events_stream__v1 = bigquery_etl_query(
        task_id="net_thunderbird_android_derived__events_stream__v1",
        destination_table="events_stream_v1",
        dataset_id="net_thunderbird_android_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jrediger@mozilla.com",
        email=[
            "ascholtz@mozilla.com",
            "jrediger@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wstuckey@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        arguments=["--billing-project", "moz-fx-data-backfill-2"],
        task_group=task_group_thunderbird_android,
    )

    org_mozilla_bergamot_derived__events_stream__v1 = bigquery_etl_query(
        task_id="org_mozilla_bergamot_derived__events_stream__v1",
        destination_table="events_stream_v1",
        dataset_id="org_mozilla_bergamot_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jrediger@mozilla.com",
        email=[
            "ascholtz@mozilla.com",
            "jrediger@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wstuckey@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        arguments=["--billing-project", "moz-fx-data-backfill-2"],
        task_group=task_group_bergamot,
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

    org_mozilla_connect_firefox_derived__events_stream__v1 = bigquery_etl_query(
        task_id="org_mozilla_connect_firefox_derived__events_stream__v1",
        destination_table="events_stream_v1",
        dataset_id="org_mozilla_connect_firefox_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jrediger@mozilla.com",
        email=[
            "ascholtz@mozilla.com",
            "jrediger@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wstuckey@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        arguments=["--billing-project", "moz-fx-data-backfill-2"],
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

    org_mozilla_fenix_derived__events_stream__v1 = bigquery_etl_query(
        task_id="org_mozilla_fenix_derived__events_stream__v1",
        destination_table="events_stream_v1",
        dataset_id="org_mozilla_fenix_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jrediger@mozilla.com",
        email=[
            "ascholtz@mozilla.com",
            "jrediger@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wstuckey@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        arguments=["--billing-project", "moz-fx-data-backfill-2"],
        task_group=task_group_fenix,
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

    org_mozilla_fenix_nightly_derived__events_stream__v1 = bigquery_etl_query(
        task_id="org_mozilla_fenix_nightly_derived__events_stream__v1",
        destination_table="events_stream_v1",
        dataset_id="org_mozilla_fenix_nightly_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jrediger@mozilla.com",
        email=[
            "ascholtz@mozilla.com",
            "jrediger@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wstuckey@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        arguments=["--billing-project", "moz-fx-data-backfill-2"],
        task_group=task_group_fenix,
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

    org_mozilla_fennec_aurora_derived__events_stream__v1 = bigquery_etl_query(
        task_id="org_mozilla_fennec_aurora_derived__events_stream__v1",
        destination_table="events_stream_v1",
        dataset_id="org_mozilla_fennec_aurora_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jrediger@mozilla.com",
        email=[
            "ascholtz@mozilla.com",
            "jrediger@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wstuckey@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        arguments=["--billing-project", "moz-fx-data-backfill-2"],
        task_group=task_group_fenix,
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

    org_mozilla_firefox_beta_derived__events_stream__v1 = bigquery_etl_query(
        task_id="org_mozilla_firefox_beta_derived__events_stream__v1",
        destination_table="events_stream_v1",
        dataset_id="org_mozilla_firefox_beta_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jrediger@mozilla.com",
        email=[
            "ascholtz@mozilla.com",
            "jrediger@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wstuckey@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        arguments=["--billing-project", "moz-fx-data-backfill-2"],
        task_group=task_group_fenix,
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

    org_mozilla_firefox_derived__events_stream__v1 = bigquery_etl_query(
        task_id="org_mozilla_firefox_derived__events_stream__v1",
        destination_table="events_stream_v1",
        dataset_id="org_mozilla_firefox_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jrediger@mozilla.com",
        email=[
            "ascholtz@mozilla.com",
            "jrediger@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wstuckey@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        arguments=["--billing-project", "moz-fx-data-backfill-2"],
        task_group=task_group_fenix,
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

    org_mozilla_firefox_vpn_derived__events_stream__v1 = bigquery_etl_query(
        task_id="org_mozilla_firefox_vpn_derived__events_stream__v1",
        destination_table="events_stream_v1",
        dataset_id="org_mozilla_firefox_vpn_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jrediger@mozilla.com",
        email=[
            "ascholtz@mozilla.com",
            "jrediger@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wstuckey@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        arguments=["--billing-project", "moz-fx-data-backfill-2"],
        task_group=task_group_mozilla_vpn,
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

    org_mozilla_firefoxreality_derived__events_stream__v1 = bigquery_etl_query(
        task_id="org_mozilla_firefoxreality_derived__events_stream__v1",
        destination_table="events_stream_v1",
        dataset_id="org_mozilla_firefoxreality_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jrediger@mozilla.com",
        email=[
            "ascholtz@mozilla.com",
            "jrediger@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wstuckey@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        arguments=["--billing-project", "moz-fx-data-backfill-2"],
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

    org_mozilla_focus_beta_derived__events_stream__v1 = bigquery_etl_query(
        task_id="org_mozilla_focus_beta_derived__events_stream__v1",
        destination_table="events_stream_v1",
        dataset_id="org_mozilla_focus_beta_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jrediger@mozilla.com",
        email=[
            "ascholtz@mozilla.com",
            "jrediger@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wstuckey@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        arguments=["--billing-project", "moz-fx-data-backfill-2"],
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

    org_mozilla_focus_derived__events_stream__v1 = bigquery_etl_query(
        task_id="org_mozilla_focus_derived__events_stream__v1",
        destination_table="events_stream_v1",
        dataset_id="org_mozilla_focus_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jrediger@mozilla.com",
        email=[
            "ascholtz@mozilla.com",
            "jrediger@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wstuckey@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        arguments=["--billing-project", "moz-fx-data-backfill-2"],
        task_group=task_group_focus_android,
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

    org_mozilla_focus_nightly_derived__events_stream__v1 = bigquery_etl_query(
        task_id="org_mozilla_focus_nightly_derived__events_stream__v1",
        destination_table="events_stream_v1",
        dataset_id="org_mozilla_focus_nightly_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jrediger@mozilla.com",
        email=[
            "ascholtz@mozilla.com",
            "jrediger@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wstuckey@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        arguments=["--billing-project", "moz-fx-data-backfill-2"],
        task_group=task_group_focus_android,
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

    org_mozilla_ios_fennec_derived__events_stream__v1 = bigquery_etl_query(
        task_id="org_mozilla_ios_fennec_derived__events_stream__v1",
        destination_table="events_stream_v1",
        dataset_id="org_mozilla_ios_fennec_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jrediger@mozilla.com",
        email=[
            "ascholtz@mozilla.com",
            "jrediger@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wstuckey@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        arguments=["--billing-project", "moz-fx-data-backfill-2"],
        task_group=task_group_firefox_ios,
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

    org_mozilla_ios_firefox_derived__events_stream__v1 = bigquery_etl_query(
        task_id="org_mozilla_ios_firefox_derived__events_stream__v1",
        destination_table="events_stream_v1",
        dataset_id="org_mozilla_ios_firefox_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jrediger@mozilla.com",
        email=[
            "ascholtz@mozilla.com",
            "jrediger@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wstuckey@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        arguments=["--billing-project", "moz-fx-data-backfill-2"],
        task_group=task_group_firefox_ios,
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

    org_mozilla_ios_firefoxbeta_derived__events_stream__v1 = bigquery_etl_query(
        task_id="org_mozilla_ios_firefoxbeta_derived__events_stream__v1",
        destination_table="events_stream_v1",
        dataset_id="org_mozilla_ios_firefoxbeta_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jrediger@mozilla.com",
        email=[
            "ascholtz@mozilla.com",
            "jrediger@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wstuckey@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        arguments=["--billing-project", "moz-fx-data-backfill-2"],
        task_group=task_group_firefox_ios,
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

    org_mozilla_ios_firefoxvpn_derived__events_stream__v1 = bigquery_etl_query(
        task_id="org_mozilla_ios_firefoxvpn_derived__events_stream__v1",
        destination_table="events_stream_v1",
        dataset_id="org_mozilla_ios_firefoxvpn_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jrediger@mozilla.com",
        email=[
            "ascholtz@mozilla.com",
            "jrediger@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wstuckey@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        arguments=["--billing-project", "moz-fx-data-backfill-2"],
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

    org_mozilla_ios_firefoxvpn_network_extension_derived__events_stream__v1 = bigquery_etl_query(
        task_id="org_mozilla_ios_firefoxvpn_network_extension_derived__events_stream__v1",
        destination_table="events_stream_v1",
        dataset_id="org_mozilla_ios_firefoxvpn_network_extension_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jrediger@mozilla.com",
        email=[
            "ascholtz@mozilla.com",
            "jrediger@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wstuckey@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        arguments=["--billing-project", "moz-fx-data-backfill-2"],
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

    org_mozilla_ios_focus_derived__events_stream__v1 = bigquery_etl_query(
        task_id="org_mozilla_ios_focus_derived__events_stream__v1",
        destination_table="events_stream_v1",
        dataset_id="org_mozilla_ios_focus_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jrediger@mozilla.com",
        email=[
            "ascholtz@mozilla.com",
            "jrediger@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wstuckey@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        arguments=["--billing-project", "moz-fx-data-backfill-2"],
        task_group=task_group_focus_ios,
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

    org_mozilla_ios_klar_derived__events_stream__v1 = bigquery_etl_query(
        task_id="org_mozilla_ios_klar_derived__events_stream__v1",
        destination_table="events_stream_v1",
        dataset_id="org_mozilla_ios_klar_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jrediger@mozilla.com",
        email=[
            "ascholtz@mozilla.com",
            "jrediger@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wstuckey@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        arguments=["--billing-project", "moz-fx-data-backfill-2"],
        task_group=task_group_klar_ios,
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

    org_mozilla_ios_lockbox_derived__events_stream__v1 = bigquery_etl_query(
        task_id="org_mozilla_ios_lockbox_derived__events_stream__v1",
        destination_table="events_stream_v1",
        dataset_id="org_mozilla_ios_lockbox_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jrediger@mozilla.com",
        email=[
            "ascholtz@mozilla.com",
            "jrediger@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wstuckey@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        arguments=["--billing-project", "moz-fx-data-backfill-2"],
        task_group=task_group_lockwise_ios,
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

    org_mozilla_klar_derived__events_stream__v1 = bigquery_etl_query(
        task_id="org_mozilla_klar_derived__events_stream__v1",
        destination_table="events_stream_v1",
        dataset_id="org_mozilla_klar_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jrediger@mozilla.com",
        email=[
            "ascholtz@mozilla.com",
            "jrediger@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wstuckey@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        arguments=["--billing-project", "moz-fx-data-backfill-2"],
        task_group=task_group_klar_android,
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

    org_mozilla_mozregression_derived__events_stream__v1 = bigquery_etl_query(
        task_id="org_mozilla_mozregression_derived__events_stream__v1",
        destination_table="events_stream_v1",
        dataset_id="org_mozilla_mozregression_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jrediger@mozilla.com",
        email=[
            "ascholtz@mozilla.com",
            "jrediger@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wstuckey@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        arguments=["--billing-project", "moz-fx-data-backfill-2"],
        task_group=task_group_mozregression,
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

    org_mozilla_reference_browser_derived__events_stream__v1 = bigquery_etl_query(
        task_id="org_mozilla_reference_browser_derived__events_stream__v1",
        destination_table="events_stream_v1",
        dataset_id="org_mozilla_reference_browser_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jrediger@mozilla.com",
        email=[
            "ascholtz@mozilla.com",
            "jrediger@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wstuckey@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        arguments=["--billing-project", "moz-fx-data-backfill-2"],
        task_group=task_group_reference_browser,
    )

    org_mozilla_social_nightly_derived__baseline_clients_daily__v1 = bigquery_etl_query(
        task_id="org_mozilla_social_nightly_derived__baseline_clients_daily__v1",
        destination_table="baseline_clients_daily_v1",
        dataset_id="org_mozilla_social_nightly_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_org_mozilla_social_nightly,
    )

    org_mozilla_social_nightly_derived__baseline_clients_first_seen__v1 = bigquery_etl_query(
        task_id="org_mozilla_social_nightly_derived__baseline_clients_first_seen__v1",
        destination_table="baseline_clients_first_seen_v1",
        dataset_id="org_mozilla_social_nightly_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=True,
        parameters=["submission_date:DATE:{{ds}}"],
        task_group=task_group_org_mozilla_social_nightly,
    )

    org_mozilla_social_nightly_derived__baseline_clients_last_seen__v1 = bigquery_etl_query(
        task_id="org_mozilla_social_nightly_derived__baseline_clients_last_seen__v1",
        destination_table="baseline_clients_last_seen_v1",
        dataset_id="org_mozilla_social_nightly_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=True,
        task_group=task_group_org_mozilla_social_nightly,
    )

    org_mozilla_social_nightly_derived__events_stream__v1 = bigquery_etl_query(
        task_id="org_mozilla_social_nightly_derived__events_stream__v1",
        destination_table="events_stream_v1",
        dataset_id="org_mozilla_social_nightly_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jrediger@mozilla.com",
        email=[
            "ascholtz@mozilla.com",
            "jrediger@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wstuckey@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        arguments=["--billing-project", "moz-fx-data-backfill-2"],
        task_group=task_group_org_mozilla_social_nightly,
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

    org_mozilla_tv_firefox_derived__events_stream__v1 = bigquery_etl_query(
        task_id="org_mozilla_tv_firefox_derived__events_stream__v1",
        destination_table="events_stream_v1",
        dataset_id="org_mozilla_tv_firefox_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jrediger@mozilla.com",
        email=[
            "ascholtz@mozilla.com",
            "jrediger@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wstuckey@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        arguments=["--billing-project", "moz-fx-data-backfill-2"],
        task_group=task_group_firefox_fire_tv,
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

    org_mozilla_vrbrowser_derived__events_stream__v1 = bigquery_etl_query(
        task_id="org_mozilla_vrbrowser_derived__events_stream__v1",
        destination_table="events_stream_v1",
        dataset_id="org_mozilla_vrbrowser_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jrediger@mozilla.com",
        email=[
            "ascholtz@mozilla.com",
            "jrediger@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wstuckey@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        arguments=["--billing-project", "moz-fx-data-backfill-2"],
        task_group=task_group_firefox_reality,
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

    pine_derived__events_stream__v1 = bigquery_etl_query(
        task_id="pine_derived__events_stream__v1",
        destination_table="events_stream_v1",
        dataset_id="pine_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jrediger@mozilla.com",
        email=[
            "ascholtz@mozilla.com",
            "jrediger@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wstuckey@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        arguments=["--billing-project", "moz-fx-data-backfill-2"],
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

    relay_backend_derived__events_stream__v1 = bigquery_etl_query(
        task_id="relay_backend_derived__events_stream__v1",
        destination_table="events_stream_v1",
        dataset_id="relay_backend_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jrediger@mozilla.com",
        email=[
            "ascholtz@mozilla.com",
            "jrediger@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wstuckey@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        arguments=["--billing-project", "moz-fx-data-backfill-2"],
        task_group=task_group_relay_backend,
    )

    syncstorage_derived__events_stream__v1 = bigquery_etl_query(
        task_id="syncstorage_derived__events_stream__v1",
        destination_table="events_stream_v1",
        dataset_id="syncstorage_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jrediger@mozilla.com",
        email=[
            "ascholtz@mozilla.com",
            "jrediger@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wstuckey@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        arguments=["--billing-project", "moz-fx-data-backfill-2"],
        task_group=task_group_syncstorage,
    )

    thunderbird_android_derived__metrics_clients_daily__v1 = bigquery_etl_query(
        task_id="thunderbird_android_derived__metrics_clients_daily__v1",
        destination_table="metrics_clients_daily_v1",
        dataset_id="thunderbird_android_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_thunderbird_android,
    )

    thunderbird_desktop_derived__baseline_clients_daily__v1 = bigquery_etl_query(
        task_id="thunderbird_desktop_derived__baseline_clients_daily__v1",
        destination_table="baseline_clients_daily_v1",
        dataset_id="thunderbird_desktop_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_thunderbird_desktop,
    )

    thunderbird_desktop_derived__baseline_clients_first_seen__v1 = bigquery_etl_query(
        task_id="thunderbird_desktop_derived__baseline_clients_first_seen__v1",
        destination_table="baseline_clients_first_seen_v1",
        dataset_id="thunderbird_desktop_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=True,
        parameters=["submission_date:DATE:{{ds}}"],
        task_group=task_group_thunderbird_desktop,
    )

    thunderbird_desktop_derived__baseline_clients_last_seen__v1 = bigquery_etl_query(
        task_id="thunderbird_desktop_derived__baseline_clients_last_seen__v1",
        destination_table="baseline_clients_last_seen_v1",
        dataset_id="thunderbird_desktop_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=True,
        task_group=task_group_thunderbird_desktop,
    )

    thunderbird_desktop_derived__events_stream__v1 = bigquery_etl_query(
        task_id="thunderbird_desktop_derived__events_stream__v1",
        destination_table="events_stream_v1",
        dataset_id="thunderbird_desktop_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jrediger@mozilla.com",
        email=[
            "ascholtz@mozilla.com",
            "jrediger@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wstuckey@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        arguments=["--billing-project", "moz-fx-data-backfill-2"],
        task_group=task_group_thunderbird_desktop,
    )

    thunderbird_desktop_derived__metrics_clients_daily__v1 = bigquery_etl_query(
        task_id="thunderbird_desktop_derived__metrics_clients_daily__v1",
        destination_table="metrics_clients_daily_v1",
        dataset_id="thunderbird_desktop_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_thunderbird_desktop,
    )

    treeherder_derived__events_stream__v1 = bigquery_etl_query(
        task_id="treeherder_derived__events_stream__v1",
        destination_table="events_stream_v1",
        dataset_id="treeherder_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jrediger@mozilla.com",
        email=[
            "ascholtz@mozilla.com",
            "jrediger@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wstuckey@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        arguments=["--billing-project", "moz-fx-data-backfill-2"],
        task_group=task_group_treeherder,
    )

    viu_politica_derived__events_stream__v1 = bigquery_etl_query(
        task_id="viu_politica_derived__events_stream__v1",
        destination_table="events_stream_v1",
        dataset_id="viu_politica_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jrediger@mozilla.com",
        email=[
            "ascholtz@mozilla.com",
            "jrediger@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wstuckey@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        arguments=["--billing-project", "moz-fx-data-backfill-2"],
        task_group=task_group_viu_politica,
    )

    accounts_backend_derived__events_stream__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    accounts_cirrus_derived__baseline_clients_daily__v1.set_upstream(
        accounts_cirrus_derived__baseline_clients_first_seen__v1
    )

    accounts_cirrus_derived__baseline_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    accounts_cirrus_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    accounts_cirrus_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_telemetry_derived__core_clients_first_seen__v1
    )

    accounts_cirrus_derived__baseline_clients_last_seen__v1.set_upstream(
        accounts_cirrus_derived__baseline_clients_daily__v1
    )

    accounts_cirrus_derived__events_stream__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    accounts_cirrus_derived__metrics_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    accounts_frontend_derived__events_stream__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    ads_backend_derived__events_stream__v1.set_upstream(wait_for_copy_deduplicate_all)

    bedrock_derived__events_stream__v1.set_upstream(wait_for_copy_deduplicate_all)

    bigeye__fenix_derived__metrics_clients_daily__v1.set_upstream(
        fenix_derived__metrics_clients_daily__v1
    )

    bigeye__fenix_derived__metrics_clients_last_seen__v1.set_upstream(
        fenix_derived__metrics_clients_last_seen__v1
    )

    bigeye__firefox_desktop_background_defaultagent_derived__baseline_clients_daily__v1.set_upstream(
        firefox_desktop_background_defaultagent_derived__baseline_clients_daily__v1
    )

    bigeye__firefox_desktop_background_defaultagent_derived__baseline_clients_first_seen__v1.set_upstream(
        firefox_desktop_background_defaultagent_derived__baseline_clients_first_seen__v1
    )

    bigeye__firefox_desktop_background_defaultagent_derived__baseline_clients_last_seen__v1.set_upstream(
        firefox_desktop_background_defaultagent_derived__baseline_clients_last_seen__v1
    )

    bigeye__firefox_desktop_background_defaultagent_derived__metrics_clients_daily__v1.set_upstream(
        firefox_desktop_background_defaultagent_derived__metrics_clients_daily__v1
    )

    bigeye__firefox_desktop_background_tasks_derived__baseline_clients_daily__v1.set_upstream(
        firefox_desktop_background_tasks_derived__baseline_clients_daily__v1
    )

    bigeye__firefox_desktop_background_tasks_derived__baseline_clients_first_seen__v1.set_upstream(
        firefox_desktop_background_tasks_derived__baseline_clients_first_seen__v1
    )

    bigeye__firefox_desktop_background_tasks_derived__baseline_clients_last_seen__v1.set_upstream(
        firefox_desktop_background_tasks_derived__baseline_clients_last_seen__v1
    )

    bigeye__firefox_desktop_background_tasks_derived__metrics_clients_daily__v1.set_upstream(
        firefox_desktop_background_tasks_derived__metrics_clients_daily__v1
    )

    bigeye__firefox_desktop_background_update_derived__baseline_clients_daily__v1.set_upstream(
        firefox_desktop_background_update_derived__baseline_clients_daily__v1
    )

    bigeye__firefox_desktop_background_update_derived__baseline_clients_first_seen__v1.set_upstream(
        firefox_desktop_background_update_derived__baseline_clients_first_seen__v1
    )

    bigeye__firefox_desktop_background_update_derived__baseline_clients_last_seen__v1.set_upstream(
        firefox_desktop_background_update_derived__baseline_clients_last_seen__v1
    )

    bigeye__firefox_desktop_background_update_derived__metrics_clients_daily__v1.set_upstream(
        firefox_desktop_background_update_derived__metrics_clients_daily__v1
    )

    bigeye__firefox_desktop_background_update_derived__metrics_clients_last_seen__v1.set_upstream(
        firefox_desktop_background_update_derived__metrics_clients_last_seen__v1
    )

    bigeye__firefox_desktop_derived__baseline_clients_daily__v1.set_upstream(
        firefox_desktop_derived__baseline_clients_daily__v1
    )

    bigeye__firefox_desktop_derived__baseline_clients_first_seen__v1.set_upstream(
        firefox_desktop_derived__baseline_clients_first_seen__v1
    )

    bigeye__firefox_desktop_derived__baseline_clients_last_seen__v1.set_upstream(
        firefox_desktop_derived__baseline_clients_last_seen__v1
    )

    bigeye__firefox_desktop_derived__metrics_clients_daily__v1.set_upstream(
        firefox_desktop_derived__metrics_clients_daily__v1
    )

    bigeye__firefox_desktop_derived__metrics_clients_last_seen__v1.set_upstream(
        firefox_desktop_derived__metrics_clients_last_seen__v1
    )

    bigeye__firefox_ios_derived__metrics_clients_daily__v1.set_upstream(
        firefox_ios_derived__metrics_clients_daily__v1
    )

    bigeye__firefox_ios_derived__metrics_clients_last_seen__v1.set_upstream(
        firefox_ios_derived__metrics_clients_last_seen__v1
    )

    bigeye__org_mozilla_fenix_derived__baseline_clients_daily__v1.set_upstream(
        org_mozilla_fenix_derived__baseline_clients_daily__v1
    )

    bigeye__org_mozilla_fenix_derived__baseline_clients_first_seen__v1.set_upstream(
        org_mozilla_fenix_derived__baseline_clients_first_seen__v1
    )

    bigeye__org_mozilla_fenix_derived__baseline_clients_last_seen__v1.set_upstream(
        org_mozilla_fenix_derived__baseline_clients_last_seen__v1
    )

    bigeye__org_mozilla_fenix_nightly_derived__baseline_clients_daily__v1.set_upstream(
        org_mozilla_fenix_nightly_derived__baseline_clients_daily__v1
    )

    bigeye__org_mozilla_fenix_nightly_derived__baseline_clients_first_seen__v1.set_upstream(
        org_mozilla_fenix_nightly_derived__baseline_clients_first_seen__v1
    )

    bigeye__org_mozilla_fenix_nightly_derived__baseline_clients_last_seen__v1.set_upstream(
        org_mozilla_fenix_nightly_derived__baseline_clients_last_seen__v1
    )

    bigeye__org_mozilla_fennec_aurora_derived__baseline_clients_daily__v1.set_upstream(
        org_mozilla_fennec_aurora_derived__baseline_clients_daily__v1
    )

    bigeye__org_mozilla_fennec_aurora_derived__baseline_clients_first_seen__v1.set_upstream(
        org_mozilla_fennec_aurora_derived__baseline_clients_first_seen__v1
    )

    bigeye__org_mozilla_fennec_aurora_derived__baseline_clients_last_seen__v1.set_upstream(
        org_mozilla_fennec_aurora_derived__baseline_clients_last_seen__v1
    )

    bigeye__org_mozilla_firefox_beta_derived__baseline_clients_daily__v1.set_upstream(
        org_mozilla_firefox_beta_derived__baseline_clients_daily__v1
    )

    bigeye__org_mozilla_firefox_beta_derived__baseline_clients_first_seen__v1.set_upstream(
        org_mozilla_firefox_beta_derived__baseline_clients_first_seen__v1
    )

    bigeye__org_mozilla_firefox_beta_derived__baseline_clients_last_seen__v1.set_upstream(
        org_mozilla_firefox_beta_derived__baseline_clients_last_seen__v1
    )

    bigeye__org_mozilla_firefox_derived__baseline_clients_daily__v1.set_upstream(
        org_mozilla_firefox_derived__baseline_clients_daily__v1
    )

    bigeye__org_mozilla_firefox_derived__baseline_clients_first_seen__v1.set_upstream(
        org_mozilla_firefox_derived__baseline_clients_first_seen__v1
    )

    bigeye__org_mozilla_firefox_derived__baseline_clients_last_seen__v1.set_upstream(
        org_mozilla_firefox_derived__baseline_clients_last_seen__v1
    )

    bigeye__org_mozilla_ios_fennec_derived__baseline_clients_daily__v1.set_upstream(
        org_mozilla_ios_fennec_derived__baseline_clients_daily__v1
    )

    bigeye__org_mozilla_ios_fennec_derived__baseline_clients_first_seen__v1.set_upstream(
        org_mozilla_ios_fennec_derived__baseline_clients_first_seen__v1
    )

    bigeye__org_mozilla_ios_fennec_derived__baseline_clients_last_seen__v1.set_upstream(
        org_mozilla_ios_fennec_derived__baseline_clients_last_seen__v1
    )

    bigeye__org_mozilla_ios_firefox_derived__baseline_clients_daily__v1.set_upstream(
        org_mozilla_ios_firefox_derived__baseline_clients_daily__v1
    )

    bigeye__org_mozilla_ios_firefox_derived__baseline_clients_first_seen__v1.set_upstream(
        org_mozilla_ios_firefox_derived__baseline_clients_first_seen__v1
    )

    bigeye__org_mozilla_ios_firefox_derived__baseline_clients_last_seen__v1.set_upstream(
        org_mozilla_ios_firefox_derived__baseline_clients_last_seen__v1
    )

    bigeye__org_mozilla_ios_firefoxbeta_derived__baseline_clients_daily__v1.set_upstream(
        org_mozilla_ios_firefoxbeta_derived__baseline_clients_daily__v1
    )

    bigeye__org_mozilla_ios_firefoxbeta_derived__baseline_clients_first_seen__v1.set_upstream(
        org_mozilla_ios_firefoxbeta_derived__baseline_clients_first_seen__v1
    )

    bigeye__org_mozilla_ios_firefoxbeta_derived__baseline_clients_last_seen__v1.set_upstream(
        org_mozilla_ios_firefoxbeta_derived__baseline_clients_last_seen__v1
    )

    bigeye__org_mozilla_ios_focus_derived__baseline_clients_last_seen__v1.set_upstream(
        org_mozilla_ios_focus_derived__baseline_clients_last_seen__v1
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

    burnham_derived__events_stream__v1.set_upstream(wait_for_copy_deduplicate_all)

    burnham_derived__metrics_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    burnham_derived__metrics_clients_last_seen__v1.set_upstream(
        burnham_derived__metrics_clients_daily__v1
    )

    checks__fail_firefox_desktop_background_defaultagent_derived__baseline_clients_last_seen__v1.set_upstream(
        firefox_desktop_background_defaultagent_derived__baseline_clients_last_seen__v1
    )

    checks__fail_firefox_desktop_background_tasks_derived__baseline_clients_last_seen__v1.set_upstream(
        firefox_desktop_background_tasks_derived__baseline_clients_last_seen__v1
    )

    checks__fail_firefox_desktop_background_update_derived__baseline_clients_last_seen__v1.set_upstream(
        firefox_desktop_background_update_derived__baseline_clients_last_seen__v1
    )

    checks__fail_firefox_desktop_derived__baseline_clients_last_seen__v1.set_upstream(
        firefox_desktop_derived__baseline_clients_last_seen__v1
    )

    checks__fail_net_thunderbird_android_beta_derived__baseline_clients_last_seen__v1.set_upstream(
        net_thunderbird_android_beta_derived__baseline_clients_last_seen__v1
    )

    checks__fail_net_thunderbird_android_derived__baseline_clients_last_seen__v1.set_upstream(
        net_thunderbird_android_derived__baseline_clients_daily__v1
    )

    checks__fail_net_thunderbird_android_derived__baseline_clients_last_seen__v1.set_upstream(
        net_thunderbird_android_derived__baseline_clients_last_seen__v1
    )

    checks__fail_org_mozilla_connect_firefox_derived__baseline_clients_last_seen__v1.set_upstream(
        org_mozilla_connect_firefox_derived__baseline_clients_last_seen__v1
    )

    checks__fail_org_mozilla_fenix_derived__baseline_clients_last_seen__v1.set_upstream(
        org_mozilla_fenix_derived__baseline_clients_last_seen__v1
    )

    checks__fail_org_mozilla_fenix_nightly_derived__baseline_clients_last_seen__v1.set_upstream(
        org_mozilla_fenix_nightly_derived__baseline_clients_last_seen__v1
    )

    checks__fail_org_mozilla_fennec_aurora_derived__baseline_clients_last_seen__v1.set_upstream(
        org_mozilla_fennec_aurora_derived__baseline_clients_last_seen__v1
    )

    checks__fail_org_mozilla_firefox_beta_derived__baseline_clients_last_seen__v1.set_upstream(
        org_mozilla_firefox_beta_derived__baseline_clients_last_seen__v1
    )

    checks__fail_org_mozilla_firefox_derived__baseline_clients_last_seen__v1.set_upstream(
        org_mozilla_firefox_derived__baseline_clients_last_seen__v1
    )

    checks__fail_org_mozilla_focus_beta_derived__baseline_clients_last_seen__v1.set_upstream(
        org_mozilla_focus_beta_derived__baseline_clients_last_seen__v1
    )

    checks__fail_org_mozilla_focus_derived__baseline_clients_last_seen__v1.set_upstream(
        org_mozilla_focus_derived__baseline_clients_last_seen__v1
    )

    checks__fail_org_mozilla_focus_nightly_derived__baseline_clients_last_seen__v1.set_upstream(
        org_mozilla_focus_nightly_derived__baseline_clients_last_seen__v1
    )

    checks__fail_org_mozilla_ios_fennec_derived__baseline_clients_last_seen__v1.set_upstream(
        org_mozilla_ios_fennec_derived__baseline_clients_last_seen__v1
    )

    checks__fail_org_mozilla_ios_firefox_derived__baseline_clients_last_seen__v1.set_upstream(
        org_mozilla_ios_firefox_derived__baseline_clients_last_seen__v1
    )

    checks__fail_org_mozilla_ios_firefoxbeta_derived__baseline_clients_last_seen__v1.set_upstream(
        org_mozilla_ios_firefoxbeta_derived__baseline_clients_last_seen__v1
    )

    checks__fail_org_mozilla_ios_focus_derived__baseline_clients_last_seen__v1.set_upstream(
        org_mozilla_ios_focus_derived__baseline_clients_last_seen__v1
    )

    checks__fail_org_mozilla_ios_klar_derived__baseline_clients_last_seen__v1.set_upstream(
        org_mozilla_ios_klar_derived__baseline_clients_last_seen__v1
    )

    checks__fail_org_mozilla_ios_lockbox_derived__baseline_clients_last_seen__v1.set_upstream(
        org_mozilla_ios_lockbox_derived__baseline_clients_last_seen__v1
    )

    checks__fail_org_mozilla_klar_derived__baseline_clients_last_seen__v1.set_upstream(
        org_mozilla_klar_derived__baseline_clients_last_seen__v1
    )

    checks__fail_pine_derived__baseline_clients_last_seen__v1.set_upstream(
        pine_derived__baseline_clients_last_seen__v1
    )

    checks__fail_thunderbird_desktop_derived__baseline_clients_last_seen__v1.set_upstream(
        thunderbird_desktop_derived__baseline_clients_last_seen__v1
    )

    checks__warn_net_thunderbird_android_daily_derived__baseline_clients_last_seen__v1.set_upstream(
        net_thunderbird_android_daily_derived__baseline_clients_last_seen__v1
    )

    checks__warn_net_thunderbird_android_derived__baseline_clients_last_seen__v1.set_upstream(
        net_thunderbird_android_derived__baseline_clients_daily__v1
    )

    checks__warn_net_thunderbird_android_derived__baseline_clients_last_seen__v1.set_upstream(
        net_thunderbird_android_derived__baseline_clients_last_seen__v1
    )

    checks__warn_org_mozilla_reference_browser_derived__baseline_clients_last_seen__v1.set_upstream(
        org_mozilla_reference_browser_derived__baseline_clients_last_seen__v1
    )

    checks__warn_org_mozilla_social_nightly_derived__baseline_clients_last_seen__v1.set_upstream(
        org_mozilla_social_nightly_derived__baseline_clients_last_seen__v1
    )

    debug_ping_view_derived__events_stream__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    fenix_derived__clients_last_seen_joined__v1.set_upstream(
        bigeye__fenix_derived__metrics_clients_last_seen__v1
    )

    fenix_derived__clients_last_seen_joined__v1.set_upstream(
        bigeye__org_mozilla_fenix_derived__baseline_clients_last_seen__v1
    )

    fenix_derived__clients_last_seen_joined__v1.set_upstream(
        bigeye__org_mozilla_fenix_nightly_derived__baseline_clients_last_seen__v1
    )

    fenix_derived__clients_last_seen_joined__v1.set_upstream(
        bigeye__org_mozilla_fennec_aurora_derived__baseline_clients_last_seen__v1
    )

    fenix_derived__clients_last_seen_joined__v1.set_upstream(
        bigeye__org_mozilla_firefox_beta_derived__baseline_clients_last_seen__v1
    )

    fenix_derived__clients_last_seen_joined__v1.set_upstream(
        bigeye__org_mozilla_firefox_derived__baseline_clients_last_seen__v1
    )

    fenix_derived__metrics_clients_daily__v1.set_upstream(wait_for_copy_deduplicate_all)

    fenix_derived__metrics_clients_last_seen__v1.set_upstream(
        bigeye__fenix_derived__metrics_clients_daily__v1
    )

    firefox_crashreporter_derived__baseline_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    firefox_crashreporter_derived__baseline_clients_daily__v1.set_upstream(
        firefox_crashreporter_derived__baseline_clients_first_seen__v1
    )

    firefox_crashreporter_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    firefox_crashreporter_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_telemetry_derived__core_clients_first_seen__v1
    )

    firefox_crashreporter_derived__baseline_clients_last_seen__v1.set_upstream(
        firefox_crashreporter_derived__baseline_clients_daily__v1
    )

    firefox_crashreporter_derived__events_stream__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    firefox_crashreporter_derived__metrics_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    firefox_desktop_background_defaultagent_derived__baseline_clients_daily__v1.set_upstream(
        bigeye__firefox_desktop_background_defaultagent_derived__baseline_clients_first_seen__v1
    )

    firefox_desktop_background_defaultagent_derived__baseline_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    firefox_desktop_background_defaultagent_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    firefox_desktop_background_defaultagent_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_telemetry_derived__core_clients_first_seen__v1
    )

    firefox_desktop_background_defaultagent_derived__baseline_clients_last_seen__v1.set_upstream(
        bigeye__firefox_desktop_background_defaultagent_derived__baseline_clients_daily__v1
    )

    firefox_desktop_background_defaultagent_derived__events_stream__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    firefox_desktop_background_defaultagent_derived__metrics_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    firefox_desktop_background_tasks_derived__baseline_clients_daily__v1.set_upstream(
        bigeye__firefox_desktop_background_tasks_derived__baseline_clients_first_seen__v1
    )

    firefox_desktop_background_tasks_derived__baseline_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    firefox_desktop_background_tasks_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    firefox_desktop_background_tasks_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_telemetry_derived__core_clients_first_seen__v1
    )

    firefox_desktop_background_tasks_derived__baseline_clients_last_seen__v1.set_upstream(
        bigeye__firefox_desktop_background_tasks_derived__baseline_clients_daily__v1
    )

    firefox_desktop_background_tasks_derived__events_stream__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    firefox_desktop_background_tasks_derived__metrics_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    firefox_desktop_background_update_derived__baseline_clients_daily__v1.set_upstream(
        bigeye__firefox_desktop_background_update_derived__baseline_clients_first_seen__v1
    )

    firefox_desktop_background_update_derived__baseline_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    firefox_desktop_background_update_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    firefox_desktop_background_update_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_telemetry_derived__core_clients_first_seen__v1
    )

    firefox_desktop_background_update_derived__baseline_clients_last_seen__v1.set_upstream(
        bigeye__firefox_desktop_background_update_derived__baseline_clients_daily__v1
    )

    firefox_desktop_background_update_derived__clients_last_seen_joined__v1.set_upstream(
        bigeye__firefox_desktop_background_update_derived__baseline_clients_last_seen__v1
    )

    firefox_desktop_background_update_derived__clients_last_seen_joined__v1.set_upstream(
        bigeye__firefox_desktop_background_update_derived__metrics_clients_last_seen__v1
    )

    firefox_desktop_background_update_derived__events_stream__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    firefox_desktop_background_update_derived__metrics_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    firefox_desktop_background_update_derived__metrics_clients_last_seen__v1.set_upstream(
        bigeye__firefox_desktop_background_update_derived__metrics_clients_daily__v1
    )

    firefox_desktop_derived__baseline_clients_daily__v1.set_upstream(
        bigeye__firefox_desktop_derived__baseline_clients_first_seen__v1
    )

    firefox_desktop_derived__baseline_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    firefox_desktop_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    firefox_desktop_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_telemetry_derived__core_clients_first_seen__v1
    )

    firefox_desktop_derived__baseline_clients_last_seen__v1.set_upstream(
        bigeye__firefox_desktop_derived__baseline_clients_daily__v1
    )

    firefox_desktop_derived__clients_last_seen_joined__v1.set_upstream(
        bigeye__firefox_desktop_derived__baseline_clients_last_seen__v1
    )

    firefox_desktop_derived__clients_last_seen_joined__v1.set_upstream(
        bigeye__firefox_desktop_derived__metrics_clients_last_seen__v1
    )

    firefox_desktop_derived__events_stream__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    firefox_desktop_derived__metrics_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    firefox_desktop_derived__metrics_clients_last_seen__v1.set_upstream(
        bigeye__firefox_desktop_derived__metrics_clients_daily__v1
    )

    firefox_echo_show_derived__clients_last_seen_joined__v1.set_upstream(
        checks__fail_org_mozilla_connect_firefox_derived__baseline_clients_last_seen__v1
    )

    firefox_echo_show_derived__clients_last_seen_joined__v1.set_upstream(
        firefox_echo_show_derived__metrics_clients_last_seen__v1
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
        bigeye__firefox_ios_derived__metrics_clients_last_seen__v1
    )

    firefox_ios_derived__clients_last_seen_joined__v1.set_upstream(
        bigeye__org_mozilla_ios_fennec_derived__baseline_clients_last_seen__v1
    )

    firefox_ios_derived__clients_last_seen_joined__v1.set_upstream(
        bigeye__org_mozilla_ios_firefox_derived__baseline_clients_last_seen__v1
    )

    firefox_ios_derived__clients_last_seen_joined__v1.set_upstream(
        bigeye__org_mozilla_ios_firefoxbeta_derived__baseline_clients_last_seen__v1
    )

    firefox_ios_derived__metrics_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    firefox_ios_derived__metrics_clients_last_seen__v1.set_upstream(
        bigeye__firefox_ios_derived__metrics_clients_daily__v1
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

    firefox_translations_derived__events_stream__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    focus_android_derived__clients_last_seen_joined__v1.set_upstream(
        checks__fail_org_mozilla_focus_beta_derived__baseline_clients_last_seen__v1
    )

    focus_android_derived__clients_last_seen_joined__v1.set_upstream(
        checks__fail_org_mozilla_focus_derived__baseline_clients_last_seen__v1
    )

    focus_android_derived__clients_last_seen_joined__v1.set_upstream(
        checks__fail_org_mozilla_focus_nightly_derived__baseline_clients_last_seen__v1
    )

    focus_android_derived__clients_last_seen_joined__v1.set_upstream(
        focus_android_derived__metrics_clients_last_seen__v1
    )

    focus_android_derived__metrics_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    focus_android_derived__metrics_clients_last_seen__v1.set_upstream(
        focus_android_derived__metrics_clients_daily__v1
    )

    focus_ios_derived__clients_last_seen_joined__v1.set_upstream(
        bigeye__org_mozilla_ios_focus_derived__baseline_clients_last_seen__v1
    )

    focus_ios_derived__clients_last_seen_joined__v1.set_upstream(
        focus_ios_derived__metrics_clients_last_seen__v1
    )

    focus_ios_derived__metrics_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    focus_ios_derived__metrics_clients_last_seen__v1.set_upstream(
        focus_ios_derived__metrics_clients_daily__v1
    )

    glean_dictionary_derived__events_stream__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    gleanjs_docs_derived__events_stream__v1.set_upstream(wait_for_copy_deduplicate_all)

    klar_android_derived__clients_last_seen_joined__v1.set_upstream(
        checks__fail_org_mozilla_klar_derived__baseline_clients_last_seen__v1
    )

    klar_android_derived__clients_last_seen_joined__v1.set_upstream(
        klar_android_derived__metrics_clients_last_seen__v1
    )

    klar_android_derived__metrics_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    klar_android_derived__metrics_clients_last_seen__v1.set_upstream(
        klar_android_derived__metrics_clients_daily__v1
    )

    klar_ios_derived__clients_last_seen_joined__v1.set_upstream(
        checks__fail_org_mozilla_ios_klar_derived__baseline_clients_last_seen__v1
    )

    klar_ios_derived__clients_last_seen_joined__v1.set_upstream(
        klar_ios_derived__metrics_clients_last_seen__v1
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
        checks__fail_org_mozilla_ios_lockbox_derived__baseline_clients_last_seen__v1
    )

    lockwise_ios_derived__clients_last_seen_joined__v1.set_upstream(
        lockwise_ios_derived__metrics_clients_last_seen__v1
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

    mdn_yari_derived__events_stream__v1.set_upstream(wait_for_copy_deduplicate_all)

    monitor_backend_derived__events_stream__v1.set_upstream(
        wait_for_copy_deduplicate_all
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

    monitor_cirrus_derived__events_stream__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    monitor_cirrus_derived__metrics_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    monitor_frontend_derived__events_stream__v1.set_upstream(
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

    mozilla_lockbox_derived__events_stream__v1.set_upstream(
        wait_for_copy_deduplicate_all
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

    mozilla_mach_derived__events_stream__v1.set_upstream(wait_for_copy_deduplicate_all)

    mozilla_vpn_derived__metrics_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    mozillavpn_backend_cirrus_derived__baseline_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    mozillavpn_backend_cirrus_derived__baseline_clients_daily__v1.set_upstream(
        mozillavpn_backend_cirrus_derived__baseline_clients_first_seen__v1
    )

    mozillavpn_backend_cirrus_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    mozillavpn_backend_cirrus_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_telemetry_derived__core_clients_first_seen__v1
    )

    mozillavpn_backend_cirrus_derived__baseline_clients_last_seen__v1.set_upstream(
        mozillavpn_backend_cirrus_derived__baseline_clients_daily__v1
    )

    mozillavpn_backend_cirrus_derived__events_stream__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    mozillavpn_backend_cirrus_derived__metrics_clients_daily__v1.set_upstream(
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

    mozillavpn_derived__events_stream__v1.set_upstream(wait_for_copy_deduplicate_all)

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

    mozphab_derived__events_stream__v1.set_upstream(wait_for_copy_deduplicate_all)

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

    net_thunderbird_android_beta_derived__baseline_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    net_thunderbird_android_beta_derived__baseline_clients_daily__v1.set_upstream(
        net_thunderbird_android_beta_derived__baseline_clients_first_seen__v1
    )

    net_thunderbird_android_beta_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    net_thunderbird_android_beta_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_telemetry_derived__core_clients_first_seen__v1
    )

    net_thunderbird_android_beta_derived__baseline_clients_last_seen__v1.set_upstream(
        net_thunderbird_android_beta_derived__baseline_clients_daily__v1
    )

    net_thunderbird_android_beta_derived__events_stream__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    net_thunderbird_android_daily_derived__baseline_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    net_thunderbird_android_daily_derived__baseline_clients_daily__v1.set_upstream(
        net_thunderbird_android_daily_derived__baseline_clients_first_seen__v1
    )

    net_thunderbird_android_daily_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    net_thunderbird_android_daily_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_telemetry_derived__core_clients_first_seen__v1
    )

    net_thunderbird_android_daily_derived__baseline_clients_last_seen__v1.set_upstream(
        net_thunderbird_android_daily_derived__baseline_clients_daily__v1
    )

    net_thunderbird_android_daily_derived__events_stream__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    net_thunderbird_android_derived__baseline_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    net_thunderbird_android_derived__baseline_clients_daily__v1.set_upstream(
        net_thunderbird_android_derived__baseline_clients_first_seen__v1
    )

    net_thunderbird_android_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    net_thunderbird_android_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_telemetry_derived__core_clients_first_seen__v1
    )

    net_thunderbird_android_derived__baseline_clients_last_seen__v1.set_upstream(
        net_thunderbird_android_derived__baseline_clients_daily__v1
    )

    net_thunderbird_android_derived__events_stream__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    org_mozilla_bergamot_derived__events_stream__v1.set_upstream(
        wait_for_copy_deduplicate_all
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

    org_mozilla_connect_firefox_derived__events_stream__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    org_mozilla_fenix_derived__baseline_clients_daily__v1.set_upstream(
        bigeye__org_mozilla_fenix_derived__baseline_clients_first_seen__v1
    )

    org_mozilla_fenix_derived__baseline_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    org_mozilla_fenix_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    org_mozilla_fenix_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_telemetry_derived__core_clients_first_seen__v1
    )

    org_mozilla_fenix_derived__baseline_clients_last_seen__v1.set_upstream(
        bigeye__org_mozilla_fenix_derived__baseline_clients_daily__v1
    )

    org_mozilla_fenix_derived__events_stream__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    org_mozilla_fenix_nightly_derived__baseline_clients_daily__v1.set_upstream(
        bigeye__org_mozilla_fenix_nightly_derived__baseline_clients_first_seen__v1
    )

    org_mozilla_fenix_nightly_derived__baseline_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    org_mozilla_fenix_nightly_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    org_mozilla_fenix_nightly_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_telemetry_derived__core_clients_first_seen__v1
    )

    org_mozilla_fenix_nightly_derived__baseline_clients_last_seen__v1.set_upstream(
        bigeye__org_mozilla_fenix_nightly_derived__baseline_clients_daily__v1
    )

    org_mozilla_fenix_nightly_derived__events_stream__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    org_mozilla_fennec_aurora_derived__baseline_clients_daily__v1.set_upstream(
        bigeye__org_mozilla_fennec_aurora_derived__baseline_clients_first_seen__v1
    )

    org_mozilla_fennec_aurora_derived__baseline_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    org_mozilla_fennec_aurora_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    org_mozilla_fennec_aurora_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_telemetry_derived__core_clients_first_seen__v1
    )

    org_mozilla_fennec_aurora_derived__baseline_clients_last_seen__v1.set_upstream(
        bigeye__org_mozilla_fennec_aurora_derived__baseline_clients_daily__v1
    )

    org_mozilla_fennec_aurora_derived__events_stream__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    org_mozilla_firefox_beta_derived__baseline_clients_daily__v1.set_upstream(
        bigeye__org_mozilla_firefox_beta_derived__baseline_clients_first_seen__v1
    )

    org_mozilla_firefox_beta_derived__baseline_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    org_mozilla_firefox_beta_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    org_mozilla_firefox_beta_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_telemetry_derived__core_clients_first_seen__v1
    )

    org_mozilla_firefox_beta_derived__baseline_clients_last_seen__v1.set_upstream(
        bigeye__org_mozilla_firefox_beta_derived__baseline_clients_daily__v1
    )

    org_mozilla_firefox_beta_derived__events_stream__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    org_mozilla_firefox_derived__baseline_clients_daily__v1.set_upstream(
        bigeye__org_mozilla_firefox_derived__baseline_clients_first_seen__v1
    )

    org_mozilla_firefox_derived__baseline_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    org_mozilla_firefox_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    org_mozilla_firefox_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_telemetry_derived__core_clients_first_seen__v1
    )

    org_mozilla_firefox_derived__baseline_clients_last_seen__v1.set_upstream(
        bigeye__org_mozilla_firefox_derived__baseline_clients_daily__v1
    )

    org_mozilla_firefox_derived__events_stream__v1.set_upstream(
        wait_for_copy_deduplicate_all
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

    org_mozilla_firefox_vpn_derived__events_stream__v1.set_upstream(
        wait_for_copy_deduplicate_all
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

    org_mozilla_firefoxreality_derived__events_stream__v1.set_upstream(
        wait_for_copy_deduplicate_all
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

    org_mozilla_focus_beta_derived__events_stream__v1.set_upstream(
        wait_for_copy_deduplicate_all
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

    org_mozilla_focus_derived__events_stream__v1.set_upstream(
        wait_for_copy_deduplicate_all
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

    org_mozilla_focus_nightly_derived__events_stream__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    org_mozilla_ios_fennec_derived__baseline_clients_daily__v1.set_upstream(
        bigeye__org_mozilla_ios_fennec_derived__baseline_clients_first_seen__v1
    )

    org_mozilla_ios_fennec_derived__baseline_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    org_mozilla_ios_fennec_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    org_mozilla_ios_fennec_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_telemetry_derived__core_clients_first_seen__v1
    )

    org_mozilla_ios_fennec_derived__baseline_clients_last_seen__v1.set_upstream(
        bigeye__org_mozilla_ios_fennec_derived__baseline_clients_daily__v1
    )

    org_mozilla_ios_fennec_derived__events_stream__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    org_mozilla_ios_firefox_derived__baseline_clients_daily__v1.set_upstream(
        bigeye__org_mozilla_ios_firefox_derived__baseline_clients_first_seen__v1
    )

    org_mozilla_ios_firefox_derived__baseline_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    org_mozilla_ios_firefox_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    org_mozilla_ios_firefox_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_telemetry_derived__core_clients_first_seen__v1
    )

    org_mozilla_ios_firefox_derived__baseline_clients_last_seen__v1.set_upstream(
        bigeye__org_mozilla_ios_firefox_derived__baseline_clients_daily__v1
    )

    org_mozilla_ios_firefox_derived__events_stream__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    org_mozilla_ios_firefoxbeta_derived__baseline_clients_daily__v1.set_upstream(
        bigeye__org_mozilla_ios_firefoxbeta_derived__baseline_clients_first_seen__v1
    )

    org_mozilla_ios_firefoxbeta_derived__baseline_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    org_mozilla_ios_firefoxbeta_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    org_mozilla_ios_firefoxbeta_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_telemetry_derived__core_clients_first_seen__v1
    )

    org_mozilla_ios_firefoxbeta_derived__baseline_clients_last_seen__v1.set_upstream(
        bigeye__org_mozilla_ios_firefoxbeta_derived__baseline_clients_daily__v1
    )

    org_mozilla_ios_firefoxbeta_derived__events_stream__v1.set_upstream(
        wait_for_copy_deduplicate_all
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

    org_mozilla_ios_firefoxvpn_derived__events_stream__v1.set_upstream(
        wait_for_copy_deduplicate_all
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

    org_mozilla_ios_firefoxvpn_network_extension_derived__events_stream__v1.set_upstream(
        wait_for_copy_deduplicate_all
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

    org_mozilla_ios_focus_derived__events_stream__v1.set_upstream(
        wait_for_copy_deduplicate_all
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

    org_mozilla_ios_klar_derived__events_stream__v1.set_upstream(
        wait_for_copy_deduplicate_all
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

    org_mozilla_ios_lockbox_derived__events_stream__v1.set_upstream(
        wait_for_copy_deduplicate_all
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

    org_mozilla_klar_derived__events_stream__v1.set_upstream(
        wait_for_copy_deduplicate_all
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

    org_mozilla_mozregression_derived__events_stream__v1.set_upstream(
        wait_for_copy_deduplicate_all
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

    org_mozilla_reference_browser_derived__events_stream__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    org_mozilla_social_nightly_derived__baseline_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    org_mozilla_social_nightly_derived__baseline_clients_daily__v1.set_upstream(
        org_mozilla_social_nightly_derived__baseline_clients_first_seen__v1
    )

    org_mozilla_social_nightly_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    org_mozilla_social_nightly_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_telemetry_derived__core_clients_first_seen__v1
    )

    org_mozilla_social_nightly_derived__baseline_clients_last_seen__v1.set_upstream(
        org_mozilla_social_nightly_derived__baseline_clients_daily__v1
    )

    org_mozilla_social_nightly_derived__events_stream__v1.set_upstream(
        wait_for_copy_deduplicate_all
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

    org_mozilla_tv_firefox_derived__events_stream__v1.set_upstream(
        wait_for_copy_deduplicate_all
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

    org_mozilla_vrbrowser_derived__events_stream__v1.set_upstream(
        wait_for_copy_deduplicate_all
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

    pine_derived__events_stream__v1.set_upstream(wait_for_copy_deduplicate_all)

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

    relay_backend_derived__events_stream__v1.set_upstream(wait_for_copy_deduplicate_all)

    syncstorage_derived__events_stream__v1.set_upstream(wait_for_copy_deduplicate_all)

    thunderbird_android_derived__metrics_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    thunderbird_desktop_derived__baseline_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    thunderbird_desktop_derived__baseline_clients_daily__v1.set_upstream(
        thunderbird_desktop_derived__baseline_clients_first_seen__v1
    )

    thunderbird_desktop_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    thunderbird_desktop_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_telemetry_derived__core_clients_first_seen__v1
    )

    thunderbird_desktop_derived__baseline_clients_last_seen__v1.set_upstream(
        thunderbird_desktop_derived__baseline_clients_daily__v1
    )

    thunderbird_desktop_derived__events_stream__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    thunderbird_desktop_derived__metrics_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    treeherder_derived__events_stream__v1.set_upstream(wait_for_copy_deduplicate_all)

    viu_politica_derived__events_stream__v1.set_upstream(wait_for_copy_deduplicate_all)
