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
    "max_active_tis_per_dag": None,
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

    task_group_experimenter_cirrus = TaskGroup("experimenter_cirrus")

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

    task_group_glam = TaskGroup("glam")

    task_group_glean_dictionary = TaskGroup("glean_dictionary")

    task_group_gleanjs_docs = TaskGroup("gleanjs_docs")

    task_group_klar_android = TaskGroup("klar_android")

    task_group_klar_ios = TaskGroup("klar_ios")

    task_group_lockwise_android = TaskGroup("lockwise_android")

    task_group_lockwise_ios = TaskGroup("lockwise_ios")

    task_group_mach = TaskGroup("mach")

    task_group_mdn_fred = TaskGroup("mdn_fred")

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

    task_group_subscription_platform_backend = TaskGroup(
        "subscription_platform_backend"
    )

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

    with TaskGroup(
        "accounts_backend_derived__events_stream__v1_external",
        parent_group=task_group_accounts_backend,
    ) as accounts_backend_derived__events_stream__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_generated_funnels__wait_for_accounts_backend_derived__events_stream__v1",
            external_dag_id="bqetl_generated_funnels",
            external_task_id="wait_for_accounts_backend_derived__events_stream__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=75600)).isoformat() }}",
        )

        accounts_backend_derived__events_stream__v1_external.set_upstream(
            accounts_backend_derived__events_stream__v1
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

    with TaskGroup(
        "accounts_frontend_derived__events_stream__v1_external",
        parent_group=task_group_accounts_frontend,
    ) as accounts_frontend_derived__events_stream__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_generated_funnels__wait_for_accounts_frontend_derived__events_stream__v1",
            external_dag_id="bqetl_generated_funnels",
            external_task_id="wait_for_accounts_frontend_derived__events_stream__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=75600)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_glean_dictionary__wait_for_accounts_frontend_derived__events_stream__v1",
            external_dag_id="bqetl_glean_dictionary",
            external_task_id="wait_for_accounts_frontend_derived__events_stream__v1",
        )

        accounts_frontend_derived__events_stream__v1_external.set_upstream(
            accounts_frontend_derived__events_stream__v1
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

    bigeye__experimenter_cirrus_derived__baseline_clients_daily__v1 = bigquery_bigeye_check(
        task_id="bigeye__experimenter_cirrus_derived__baseline_clients_daily__v1",
        table_id="moz-fx-data-shared-prod.experimenter_cirrus_derived.baseline_clients_daily_v1",
        warehouse_id="1939",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
        task_group=task_group_experimenter_cirrus,
    )

    bigeye__experimenter_cirrus_derived__baseline_clients_first_seen__v1 = bigquery_bigeye_check(
        task_id="bigeye__experimenter_cirrus_derived__baseline_clients_first_seen__v1",
        table_id="moz-fx-data-shared-prod.experimenter_cirrus_derived.baseline_clients_first_seen_v1",
        warehouse_id="1939",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
        task_group=task_group_experimenter_cirrus,
    )

    bigeye__experimenter_cirrus_derived__baseline_clients_last_seen__v1 = bigquery_bigeye_check(
        task_id="bigeye__experimenter_cirrus_derived__baseline_clients_last_seen__v1",
        table_id="moz-fx-data-shared-prod.experimenter_cirrus_derived.baseline_clients_last_seen_v1",
        warehouse_id="1939",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
        task_group=task_group_experimenter_cirrus,
    )

    bigeye__experimenter_cirrus_derived__metrics_clients_daily__v1 = bigquery_bigeye_check(
        task_id="bigeye__experimenter_cirrus_derived__metrics_clients_daily__v1",
        table_id="moz-fx-data-shared-prod.experimenter_cirrus_derived.metrics_clients_daily_v1",
        warehouse_id="1939",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
        task_group=task_group_experimenter_cirrus,
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

    with TaskGroup(
        "bigeye__fenix_derived__metrics_clients_daily__v1_external",
        parent_group=task_group_fenix,
    ) as bigeye__fenix_derived__metrics_clients_daily__v1_external:
        ExternalTaskMarker(
            task_id="private_bqetl_device_partnerships__wait_for_bigeye__fenix_derived__metrics_clients_daily__v1",
            external_dag_id="private_bqetl_device_partnerships",
            external_task_id="wait_for_bigeye__fenix_derived__metrics_clients_daily__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=57600)).isoformat() }}",
        )

        bigeye__fenix_derived__metrics_clients_daily__v1_external.set_upstream(
            bigeye__fenix_derived__metrics_clients_daily__v1
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

    with TaskGroup(
        "bigeye__fenix_derived__metrics_clients_last_seen__v1_external",
        parent_group=task_group_fenix,
    ) as bigeye__fenix_derived__metrics_clients_last_seen__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_analytics_aggregations__wait_for_bigeye__fenix_derived__metrics_clients_last_seen__v1",
            external_dag_id="bqetl_analytics_aggregations",
            external_task_id="wait_for_bigeye__fenix_derived__metrics_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=78300)).isoformat() }}",
        )

        bigeye__fenix_derived__metrics_clients_last_seen__v1_external.set_upstream(
            bigeye__fenix_derived__metrics_clients_last_seen__v1
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

    with TaskGroup(
        "bigeye__firefox_desktop_derived__baseline_clients_daily__v1_external",
        parent_group=task_group_firefox_desktop,
    ) as bigeye__firefox_desktop_derived__baseline_clients_daily__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_analytics_tables__wait_for_bigeye__firefox_desktop_derived__baseline_clients_daily__v1",
            external_dag_id="bqetl_analytics_tables",
            external_task_id="wait_for_bigeye__firefox_desktop_derived__baseline_clients_daily__v1",
        )

        ExternalTaskMarker(
            task_id="bqetl_desktop_retention_model__wait_for_bigeye__firefox_desktop_derived__baseline_clients_daily__v1",
            external_dag_id="bqetl_desktop_retention_model",
            external_task_id="wait_for_bigeye__firefox_desktop_derived__baseline_clients_daily__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=75600)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_client_attributes__wait_for_bigeye__firefox_desktop_derived__baseline_clients_daily__v1",
            external_dag_id="bqetl_client_attributes",
            external_task_id="wait_for_bigeye__firefox_desktop_derived__baseline_clients_daily__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=22800)).isoformat() }}",
        )

        bigeye__firefox_desktop_derived__baseline_clients_daily__v1_external.set_upstream(
            bigeye__firefox_desktop_derived__baseline_clients_daily__v1
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

    with TaskGroup(
        "bigeye__firefox_desktop_derived__baseline_clients_first_seen__v1_external",
        parent_group=task_group_firefox_desktop,
    ) as bigeye__firefox_desktop_derived__baseline_clients_first_seen__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_analytics_aggregations__wait_for_bigeye__firefox_desktop_derived__baseline_clients_first_seen__v1",
            external_dag_id="bqetl_analytics_aggregations",
            external_task_id="wait_for_bigeye__firefox_desktop_derived__baseline_clients_first_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=78300)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_desktop_engagement_model__wait_for_bigeye__firefox_desktop_derived__baseline_clients_first_seen__v1",
            external_dag_id="bqetl_desktop_engagement_model",
            external_task_id="wait_for_bigeye__firefox_desktop_derived__baseline_clients_first_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=75600)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_desktop_retention_model__wait_for_bigeye__firefox_desktop_derived__baseline_clients_first_seen__v1",
            external_dag_id="bqetl_desktop_retention_model",
            external_task_id="wait_for_bigeye__firefox_desktop_derived__baseline_clients_first_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=75600)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_firefox_desktop_new_profiles_aggregates__wait_for_bigeye__firefox_desktop_derived__baseline_clients_first_seen__v1",
            external_dag_id="bqetl_firefox_desktop_new_profiles_aggregates",
            external_task_id="wait_for_bigeye__firefox_desktop_derived__baseline_clients_first_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=36000)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_cohort_retention__wait_for_bigeye__firefox_desktop_derived__baseline_clients_first_seen__v1",
            external_dag_id="bqetl_cohort_retention",
            external_task_id="wait_for_bigeye__firefox_desktop_derived__baseline_clients_first_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=22800)).isoformat() }}",
        )

        bigeye__firefox_desktop_derived__baseline_clients_first_seen__v1_external.set_upstream(
            bigeye__firefox_desktop_derived__baseline_clients_first_seen__v1
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

    with TaskGroup(
        "bigeye__firefox_desktop_derived__baseline_clients_last_seen__v1_external",
        parent_group=task_group_firefox_desktop,
    ) as bigeye__firefox_desktop_derived__baseline_clients_last_seen__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_analytics_aggregations__wait_for_bigeye__firefox_desktop_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_analytics_aggregations",
            external_task_id="wait_for_bigeye__firefox_desktop_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=78300)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_desktop_engagement_model__wait_for_bigeye__firefox_desktop_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_desktop_engagement_model",
            external_task_id="wait_for_bigeye__firefox_desktop_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=75600)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_desktop_retention_model__wait_for_bigeye__firefox_desktop_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_desktop_retention_model",
            external_task_id="wait_for_bigeye__firefox_desktop_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=75600)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_firefox_desktop_new_profiles_aggregates__wait_for_bigeye__firefox_desktop_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_firefox_desktop_new_profiles_aggregates",
            external_task_id="wait_for_bigeye__firefox_desktop_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=36000)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_cohort_retention__wait_for_bigeye__firefox_desktop_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_cohort_retention",
            external_task_id="wait_for_bigeye__firefox_desktop_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=22800)).isoformat() }}",
        )

        bigeye__firefox_desktop_derived__baseline_clients_last_seen__v1_external.set_upstream(
            bigeye__firefox_desktop_derived__baseline_clients_last_seen__v1
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

    with TaskGroup(
        "bigeye__firefox_ios_derived__metrics_clients_last_seen__v1_external",
        parent_group=task_group_firefox_ios,
    ) as bigeye__firefox_ios_derived__metrics_clients_last_seen__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_analytics_aggregations__wait_for_bigeye__firefox_ios_derived__metrics_clients_last_seen__v1",
            external_dag_id="bqetl_analytics_aggregations",
            external_task_id="wait_for_bigeye__firefox_ios_derived__metrics_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=78300)).isoformat() }}",
        )

        bigeye__firefox_ios_derived__metrics_clients_last_seen__v1_external.set_upstream(
            bigeye__firefox_ios_derived__metrics_clients_last_seen__v1
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

    with TaskGroup(
        "bigeye__org_mozilla_fenix_derived__baseline_clients_daily__v1_external",
        parent_group=task_group_fenix,
    ) as bigeye__org_mozilla_fenix_derived__baseline_clients_daily__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_org_mozilla_firefox_derived__wait_for_bigeye__org_mozilla_fenix_derived__baseline_clients_daily__v1",
            external_dag_id="bqetl_org_mozilla_firefox_derived",
            external_task_id="wait_for_bigeye__org_mozilla_fenix_derived__baseline_clients_daily__v1",
        )

        ExternalTaskMarker(
            task_id="bqetl_mobile_kpi_metrics__wait_for_bigeye__org_mozilla_fenix_derived__baseline_clients_daily__v1",
            external_dag_id="bqetl_mobile_kpi_metrics",
            external_task_id="wait_for_bigeye__org_mozilla_fenix_derived__baseline_clients_daily__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=50400)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_mobile_feature_usage__wait_for_bigeye__org_mozilla_fenix_derived__baseline_clients_daily__v1",
            external_dag_id="bqetl_mobile_feature_usage",
            external_task_id="wait_for_bigeye__org_mozilla_fenix_derived__baseline_clients_daily__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=50400)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_mobile_activation__wait_for_bigeye__org_mozilla_fenix_derived__baseline_clients_daily__v1",
            external_dag_id="bqetl_mobile_activation",
            external_task_id="wait_for_bigeye__org_mozilla_fenix_derived__baseline_clients_daily__v1",
            execution_date="{{ (execution_date - macros.timedelta(seconds=7200)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_marketing_analysis__wait_for_bigeye__org_mozilla_fenix_derived__baseline_clients_daily__v1",
            external_dag_id="bqetl_marketing_analysis",
            external_task_id="wait_for_bigeye__org_mozilla_fenix_derived__baseline_clients_daily__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=50400)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_analytics_tables__wait_for_bigeye__org_mozilla_fenix_derived__baseline_clients_daily__v1",
            external_dag_id="bqetl_analytics_tables",
            external_task_id="wait_for_bigeye__org_mozilla_fenix_derived__baseline_clients_daily__v1",
        )

        bigeye__org_mozilla_fenix_derived__baseline_clients_daily__v1_external.set_upstream(
            bigeye__org_mozilla_fenix_derived__baseline_clients_daily__v1
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

    with TaskGroup(
        "bigeye__org_mozilla_fenix_derived__baseline_clients_last_seen__v1_external",
        parent_group=task_group_fenix,
    ) as bigeye__org_mozilla_fenix_derived__baseline_clients_last_seen__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_analytics_aggregations__wait_for_bigeye__org_mozilla_fenix_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_analytics_aggregations",
            external_task_id="wait_for_bigeye__org_mozilla_fenix_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=78300)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_analytics_tables__wait_for_bigeye__org_mozilla_fenix_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_analytics_tables",
            external_task_id="wait_for_bigeye__org_mozilla_fenix_derived__baseline_clients_last_seen__v1",
        )

        ExternalTaskMarker(
            task_id="private_bqetl_device_partnerships__wait_for_bigeye__org_mozilla_fenix_derived__baseline_clients_last_seen__v1",
            external_dag_id="private_bqetl_device_partnerships",
            external_task_id="wait_for_bigeye__org_mozilla_fenix_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=57600)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_mobile_kpi_metrics__wait_for_bigeye__org_mozilla_fenix_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_mobile_kpi_metrics",
            external_task_id="wait_for_bigeye__org_mozilla_fenix_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=50400)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_mobile_activation__wait_for_bigeye__org_mozilla_fenix_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_mobile_activation",
            external_task_id="wait_for_bigeye__org_mozilla_fenix_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(seconds=7200)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_marketing_analysis__wait_for_bigeye__org_mozilla_fenix_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_marketing_analysis",
            external_task_id="wait_for_bigeye__org_mozilla_fenix_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=50400)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_fivetran_google_ads__wait_for_bigeye__org_mozilla_fenix_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_fivetran_google_ads",
            external_task_id="wait_for_bigeye__org_mozilla_fenix_derived__baseline_clients_last_seen__v1",
        )

        ExternalTaskMarker(
            task_id="bqetl_search_dashboard__wait_for_bigeye__org_mozilla_fenix_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_search_dashboard",
            external_task_id="wait_for_bigeye__org_mozilla_fenix_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=73800)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_shredder_impact_measurement__wait_for_bigeye__org_mozilla_fenix_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_shredder_impact_measurement",
            external_task_id="wait_for_bigeye__org_mozilla_fenix_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=48000)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_cohort_daily_churn__wait_for_bigeye__org_mozilla_fenix_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_cohort_daily_churn",
            external_task_id="wait_for_bigeye__org_mozilla_fenix_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=40200)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_cohort_retention__wait_for_bigeye__org_mozilla_fenix_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_cohort_retention",
            external_task_id="wait_for_bigeye__org_mozilla_fenix_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=22800)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_nondesktop__wait_for_bigeye__org_mozilla_fenix_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_nondesktop",
            external_task_id="wait_for_bigeye__org_mozilla_fenix_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=82800)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_gud__wait_for_bigeye__org_mozilla_fenix_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_gud",
            external_task_id="wait_for_bigeye__org_mozilla_fenix_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=82800)).isoformat() }}",
        )

        bigeye__org_mozilla_fenix_derived__baseline_clients_last_seen__v1_external.set_upstream(
            bigeye__org_mozilla_fenix_derived__baseline_clients_last_seen__v1
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

    with TaskGroup(
        "bigeye__org_mozilla_fenix_nightly_derived__baseline_clients_daily__v1_external",
        parent_group=task_group_fenix,
    ) as bigeye__org_mozilla_fenix_nightly_derived__baseline_clients_daily__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_org_mozilla_firefox_derived__wait_for_bigeye__org_mozilla_fenix_nightly_derived__baseline_clients_daily__v1",
            external_dag_id="bqetl_org_mozilla_firefox_derived",
            external_task_id="wait_for_bigeye__org_mozilla_fenix_nightly_derived__baseline_clients_daily__v1",
        )

        ExternalTaskMarker(
            task_id="bqetl_mobile_kpi_metrics__wait_for_bigeye__org_mozilla_fenix_nightly_derived__baseline_clients_daily__v1",
            external_dag_id="bqetl_mobile_kpi_metrics",
            external_task_id="wait_for_bigeye__org_mozilla_fenix_nightly_derived__baseline_clients_daily__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=50400)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_mobile_feature_usage__wait_for_bigeye__org_mozilla_fenix_nightly_derived__baseline_clients_daily__v1",
            external_dag_id="bqetl_mobile_feature_usage",
            external_task_id="wait_for_bigeye__org_mozilla_fenix_nightly_derived__baseline_clients_daily__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=50400)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_mobile_activation__wait_for_bigeye__org_mozilla_fenix_nightly_derived__baseline_clients_daily__v1",
            external_dag_id="bqetl_mobile_activation",
            external_task_id="wait_for_bigeye__org_mozilla_fenix_nightly_derived__baseline_clients_daily__v1",
            execution_date="{{ (execution_date - macros.timedelta(seconds=7200)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_marketing_analysis__wait_for_bigeye__org_mozilla_fenix_nightly_derived__baseline_clients_daily__v1",
            external_dag_id="bqetl_marketing_analysis",
            external_task_id="wait_for_bigeye__org_mozilla_fenix_nightly_derived__baseline_clients_daily__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=50400)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_analytics_tables__wait_for_bigeye__org_mozilla_fenix_nightly_derived__baseline_clients_daily__v1",
            external_dag_id="bqetl_analytics_tables",
            external_task_id="wait_for_bigeye__org_mozilla_fenix_nightly_derived__baseline_clients_daily__v1",
        )

        bigeye__org_mozilla_fenix_nightly_derived__baseline_clients_daily__v1_external.set_upstream(
            bigeye__org_mozilla_fenix_nightly_derived__baseline_clients_daily__v1
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

    with TaskGroup(
        "bigeye__org_mozilla_fenix_nightly_derived__baseline_clients_last_seen__v1_external",
        parent_group=task_group_fenix,
    ) as bigeye__org_mozilla_fenix_nightly_derived__baseline_clients_last_seen__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_analytics_aggregations__wait_for_bigeye__org_mozilla_fenix_nightly_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_analytics_aggregations",
            external_task_id="wait_for_bigeye__org_mozilla_fenix_nightly_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=78300)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_analytics_tables__wait_for_bigeye__org_mozilla_fenix_nightly_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_analytics_tables",
            external_task_id="wait_for_bigeye__org_mozilla_fenix_nightly_derived__baseline_clients_last_seen__v1",
        )

        ExternalTaskMarker(
            task_id="private_bqetl_device_partnerships__wait_for_bigeye__org_mozilla_fenix_nightly_derived__baseline_clients_last_seen__v1",
            external_dag_id="private_bqetl_device_partnerships",
            external_task_id="wait_for_bigeye__org_mozilla_fenix_nightly_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=57600)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_mobile_kpi_metrics__wait_for_bigeye__org_mozilla_fenix_nightly_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_mobile_kpi_metrics",
            external_task_id="wait_for_bigeye__org_mozilla_fenix_nightly_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=50400)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_mobile_activation__wait_for_bigeye__org_mozilla_fenix_nightly_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_mobile_activation",
            external_task_id="wait_for_bigeye__org_mozilla_fenix_nightly_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(seconds=7200)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_marketing_analysis__wait_for_bigeye__org_mozilla_fenix_nightly_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_marketing_analysis",
            external_task_id="wait_for_bigeye__org_mozilla_fenix_nightly_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=50400)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_fivetran_google_ads__wait_for_bigeye__org_mozilla_fenix_nightly_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_fivetran_google_ads",
            external_task_id="wait_for_bigeye__org_mozilla_fenix_nightly_derived__baseline_clients_last_seen__v1",
        )

        ExternalTaskMarker(
            task_id="bqetl_search_dashboard__wait_for_bigeye__org_mozilla_fenix_nightly_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_search_dashboard",
            external_task_id="wait_for_bigeye__org_mozilla_fenix_nightly_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=73800)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_shredder_impact_measurement__wait_for_bigeye__org_mozilla_fenix_nightly_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_shredder_impact_measurement",
            external_task_id="wait_for_bigeye__org_mozilla_fenix_nightly_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=48000)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_cohort_daily_churn__wait_for_bigeye__org_mozilla_fenix_nightly_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_cohort_daily_churn",
            external_task_id="wait_for_bigeye__org_mozilla_fenix_nightly_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=40200)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_cohort_retention__wait_for_bigeye__org_mozilla_fenix_nightly_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_cohort_retention",
            external_task_id="wait_for_bigeye__org_mozilla_fenix_nightly_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=22800)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_nondesktop__wait_for_bigeye__org_mozilla_fenix_nightly_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_nondesktop",
            external_task_id="wait_for_bigeye__org_mozilla_fenix_nightly_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=82800)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_gud__wait_for_bigeye__org_mozilla_fenix_nightly_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_gud",
            external_task_id="wait_for_bigeye__org_mozilla_fenix_nightly_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=82800)).isoformat() }}",
        )

        bigeye__org_mozilla_fenix_nightly_derived__baseline_clients_last_seen__v1_external.set_upstream(
            bigeye__org_mozilla_fenix_nightly_derived__baseline_clients_last_seen__v1
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

    with TaskGroup(
        "bigeye__org_mozilla_fennec_aurora_derived__baseline_clients_daily__v1_external",
        parent_group=task_group_fenix,
    ) as bigeye__org_mozilla_fennec_aurora_derived__baseline_clients_daily__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_org_mozilla_firefox_derived__wait_for_bigeye__org_mozilla_fennec_aurora_derived__baseline_clients_daily__v1",
            external_dag_id="bqetl_org_mozilla_firefox_derived",
            external_task_id="wait_for_bigeye__org_mozilla_fennec_aurora_derived__baseline_clients_daily__v1",
        )

        ExternalTaskMarker(
            task_id="bqetl_mobile_kpi_metrics__wait_for_bigeye__org_mozilla_fennec_aurora_derived__baseline_clients_daily__v1",
            external_dag_id="bqetl_mobile_kpi_metrics",
            external_task_id="wait_for_bigeye__org_mozilla_fennec_aurora_derived__baseline_clients_daily__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=50400)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_mobile_feature_usage__wait_for_bigeye__org_mozilla_fennec_aurora_derived__baseline_clients_daily__v1",
            external_dag_id="bqetl_mobile_feature_usage",
            external_task_id="wait_for_bigeye__org_mozilla_fennec_aurora_derived__baseline_clients_daily__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=50400)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_mobile_activation__wait_for_bigeye__org_mozilla_fennec_aurora_derived__baseline_clients_daily__v1",
            external_dag_id="bqetl_mobile_activation",
            external_task_id="wait_for_bigeye__org_mozilla_fennec_aurora_derived__baseline_clients_daily__v1",
            execution_date="{{ (execution_date - macros.timedelta(seconds=7200)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_marketing_analysis__wait_for_bigeye__org_mozilla_fennec_aurora_derived__baseline_clients_daily__v1",
            external_dag_id="bqetl_marketing_analysis",
            external_task_id="wait_for_bigeye__org_mozilla_fennec_aurora_derived__baseline_clients_daily__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=50400)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_analytics_tables__wait_for_bigeye__org_mozilla_fennec_aurora_derived__baseline_clients_daily__v1",
            external_dag_id="bqetl_analytics_tables",
            external_task_id="wait_for_bigeye__org_mozilla_fennec_aurora_derived__baseline_clients_daily__v1",
        )

        bigeye__org_mozilla_fennec_aurora_derived__baseline_clients_daily__v1_external.set_upstream(
            bigeye__org_mozilla_fennec_aurora_derived__baseline_clients_daily__v1
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

    with TaskGroup(
        "bigeye__org_mozilla_fennec_aurora_derived__baseline_clients_last_seen__v1_external",
        parent_group=task_group_fenix,
    ) as bigeye__org_mozilla_fennec_aurora_derived__baseline_clients_last_seen__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_analytics_aggregations__wait_for_bigeye__org_mozilla_fennec_aurora_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_analytics_aggregations",
            external_task_id="wait_for_bigeye__org_mozilla_fennec_aurora_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=78300)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_analytics_tables__wait_for_bigeye__org_mozilla_fennec_aurora_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_analytics_tables",
            external_task_id="wait_for_bigeye__org_mozilla_fennec_aurora_derived__baseline_clients_last_seen__v1",
        )

        ExternalTaskMarker(
            task_id="private_bqetl_device_partnerships__wait_for_bigeye__org_mozilla_fennec_aurora_derived__baseline_clients_last_seen__v1",
            external_dag_id="private_bqetl_device_partnerships",
            external_task_id="wait_for_bigeye__org_mozilla_fennec_aurora_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=57600)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_mobile_kpi_metrics__wait_for_bigeye__org_mozilla_fennec_aurora_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_mobile_kpi_metrics",
            external_task_id="wait_for_bigeye__org_mozilla_fennec_aurora_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=50400)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_mobile_activation__wait_for_bigeye__org_mozilla_fennec_aurora_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_mobile_activation",
            external_task_id="wait_for_bigeye__org_mozilla_fennec_aurora_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(seconds=7200)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_marketing_analysis__wait_for_bigeye__org_mozilla_fennec_aurora_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_marketing_analysis",
            external_task_id="wait_for_bigeye__org_mozilla_fennec_aurora_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=50400)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_fivetran_google_ads__wait_for_bigeye__org_mozilla_fennec_aurora_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_fivetran_google_ads",
            external_task_id="wait_for_bigeye__org_mozilla_fennec_aurora_derived__baseline_clients_last_seen__v1",
        )

        ExternalTaskMarker(
            task_id="bqetl_search_dashboard__wait_for_bigeye__org_mozilla_fennec_aurora_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_search_dashboard",
            external_task_id="wait_for_bigeye__org_mozilla_fennec_aurora_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=73800)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_shredder_impact_measurement__wait_for_bigeye__org_mozilla_fennec_aurora_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_shredder_impact_measurement",
            external_task_id="wait_for_bigeye__org_mozilla_fennec_aurora_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=48000)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_cohort_daily_churn__wait_for_bigeye__org_mozilla_fennec_aurora_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_cohort_daily_churn",
            external_task_id="wait_for_bigeye__org_mozilla_fennec_aurora_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=40200)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_cohort_retention__wait_for_bigeye__org_mozilla_fennec_aurora_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_cohort_retention",
            external_task_id="wait_for_bigeye__org_mozilla_fennec_aurora_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=22800)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_nondesktop__wait_for_bigeye__org_mozilla_fennec_aurora_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_nondesktop",
            external_task_id="wait_for_bigeye__org_mozilla_fennec_aurora_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=82800)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_gud__wait_for_bigeye__org_mozilla_fennec_aurora_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_gud",
            external_task_id="wait_for_bigeye__org_mozilla_fennec_aurora_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=82800)).isoformat() }}",
        )

        bigeye__org_mozilla_fennec_aurora_derived__baseline_clients_last_seen__v1_external.set_upstream(
            bigeye__org_mozilla_fennec_aurora_derived__baseline_clients_last_seen__v1
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

    with TaskGroup(
        "bigeye__org_mozilla_firefox_beta_derived__baseline_clients_daily__v1_external",
        parent_group=task_group_fenix,
    ) as bigeye__org_mozilla_firefox_beta_derived__baseline_clients_daily__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_org_mozilla_firefox_derived__wait_for_bigeye__org_mozilla_firefox_beta_derived__baseline_clients_daily__v1",
            external_dag_id="bqetl_org_mozilla_firefox_derived",
            external_task_id="wait_for_bigeye__org_mozilla_firefox_beta_derived__baseline_clients_daily__v1",
        )

        ExternalTaskMarker(
            task_id="bqetl_mobile_kpi_metrics__wait_for_bigeye__org_mozilla_firefox_beta_derived__baseline_clients_daily__v1",
            external_dag_id="bqetl_mobile_kpi_metrics",
            external_task_id="wait_for_bigeye__org_mozilla_firefox_beta_derived__baseline_clients_daily__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=50400)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_mobile_feature_usage__wait_for_bigeye__org_mozilla_firefox_beta_derived__baseline_clients_daily__v1",
            external_dag_id="bqetl_mobile_feature_usage",
            external_task_id="wait_for_bigeye__org_mozilla_firefox_beta_derived__baseline_clients_daily__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=50400)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_mobile_activation__wait_for_bigeye__org_mozilla_firefox_beta_derived__baseline_clients_daily__v1",
            external_dag_id="bqetl_mobile_activation",
            external_task_id="wait_for_bigeye__org_mozilla_firefox_beta_derived__baseline_clients_daily__v1",
            execution_date="{{ (execution_date - macros.timedelta(seconds=7200)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_marketing_analysis__wait_for_bigeye__org_mozilla_firefox_beta_derived__baseline_clients_daily__v1",
            external_dag_id="bqetl_marketing_analysis",
            external_task_id="wait_for_bigeye__org_mozilla_firefox_beta_derived__baseline_clients_daily__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=50400)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_analytics_tables__wait_for_bigeye__org_mozilla_firefox_beta_derived__baseline_clients_daily__v1",
            external_dag_id="bqetl_analytics_tables",
            external_task_id="wait_for_bigeye__org_mozilla_firefox_beta_derived__baseline_clients_daily__v1",
        )

        bigeye__org_mozilla_firefox_beta_derived__baseline_clients_daily__v1_external.set_upstream(
            bigeye__org_mozilla_firefox_beta_derived__baseline_clients_daily__v1
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

    with TaskGroup(
        "bigeye__org_mozilla_firefox_beta_derived__baseline_clients_last_seen__v1_external",
        parent_group=task_group_fenix,
    ) as bigeye__org_mozilla_firefox_beta_derived__baseline_clients_last_seen__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_analytics_aggregations__wait_for_bigeye__org_mozilla_firefox_beta_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_analytics_aggregations",
            external_task_id="wait_for_bigeye__org_mozilla_firefox_beta_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=78300)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_analytics_tables__wait_for_bigeye__org_mozilla_firefox_beta_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_analytics_tables",
            external_task_id="wait_for_bigeye__org_mozilla_firefox_beta_derived__baseline_clients_last_seen__v1",
        )

        ExternalTaskMarker(
            task_id="private_bqetl_device_partnerships__wait_for_bigeye__org_mozilla_firefox_beta_derived__baseline_clients_last_seen__v1",
            external_dag_id="private_bqetl_device_partnerships",
            external_task_id="wait_for_bigeye__org_mozilla_firefox_beta_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=57600)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_mobile_kpi_metrics__wait_for_bigeye__org_mozilla_firefox_beta_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_mobile_kpi_metrics",
            external_task_id="wait_for_bigeye__org_mozilla_firefox_beta_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=50400)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_mobile_activation__wait_for_bigeye__org_mozilla_firefox_beta_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_mobile_activation",
            external_task_id="wait_for_bigeye__org_mozilla_firefox_beta_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(seconds=7200)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_marketing_analysis__wait_for_bigeye__org_mozilla_firefox_beta_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_marketing_analysis",
            external_task_id="wait_for_bigeye__org_mozilla_firefox_beta_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=50400)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_fivetran_google_ads__wait_for_bigeye__org_mozilla_firefox_beta_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_fivetran_google_ads",
            external_task_id="wait_for_bigeye__org_mozilla_firefox_beta_derived__baseline_clients_last_seen__v1",
        )

        ExternalTaskMarker(
            task_id="bqetl_search_dashboard__wait_for_bigeye__org_mozilla_firefox_beta_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_search_dashboard",
            external_task_id="wait_for_bigeye__org_mozilla_firefox_beta_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=73800)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_shredder_impact_measurement__wait_for_bigeye__org_mozilla_firefox_beta_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_shredder_impact_measurement",
            external_task_id="wait_for_bigeye__org_mozilla_firefox_beta_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=48000)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_cohort_daily_churn__wait_for_bigeye__org_mozilla_firefox_beta_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_cohort_daily_churn",
            external_task_id="wait_for_bigeye__org_mozilla_firefox_beta_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=40200)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_cohort_retention__wait_for_bigeye__org_mozilla_firefox_beta_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_cohort_retention",
            external_task_id="wait_for_bigeye__org_mozilla_firefox_beta_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=22800)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_nondesktop__wait_for_bigeye__org_mozilla_firefox_beta_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_nondesktop",
            external_task_id="wait_for_bigeye__org_mozilla_firefox_beta_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=82800)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_gud__wait_for_bigeye__org_mozilla_firefox_beta_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_gud",
            external_task_id="wait_for_bigeye__org_mozilla_firefox_beta_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=82800)).isoformat() }}",
        )

        bigeye__org_mozilla_firefox_beta_derived__baseline_clients_last_seen__v1_external.set_upstream(
            bigeye__org_mozilla_firefox_beta_derived__baseline_clients_last_seen__v1
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

    with TaskGroup(
        "bigeye__org_mozilla_firefox_derived__baseline_clients_daily__v1_external",
        parent_group=task_group_fenix,
    ) as bigeye__org_mozilla_firefox_derived__baseline_clients_daily__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_org_mozilla_firefox_derived__wait_for_bigeye__org_mozilla_firefox_derived__baseline_clients_daily__v1",
            external_dag_id="bqetl_org_mozilla_firefox_derived",
            external_task_id="wait_for_bigeye__org_mozilla_firefox_derived__baseline_clients_daily__v1",
        )

        ExternalTaskMarker(
            task_id="bqetl_mobile_kpi_metrics__wait_for_bigeye__org_mozilla_firefox_derived__baseline_clients_daily__v1",
            external_dag_id="bqetl_mobile_kpi_metrics",
            external_task_id="wait_for_bigeye__org_mozilla_firefox_derived__baseline_clients_daily__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=50400)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_mobile_feature_usage__wait_for_bigeye__org_mozilla_firefox_derived__baseline_clients_daily__v1",
            external_dag_id="bqetl_mobile_feature_usage",
            external_task_id="wait_for_bigeye__org_mozilla_firefox_derived__baseline_clients_daily__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=50400)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_mobile_activation__wait_for_bigeye__org_mozilla_firefox_derived__baseline_clients_daily__v1",
            external_dag_id="bqetl_mobile_activation",
            external_task_id="wait_for_bigeye__org_mozilla_firefox_derived__baseline_clients_daily__v1",
            execution_date="{{ (execution_date - macros.timedelta(seconds=7200)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_marketing_analysis__wait_for_bigeye__org_mozilla_firefox_derived__baseline_clients_daily__v1",
            external_dag_id="bqetl_marketing_analysis",
            external_task_id="wait_for_bigeye__org_mozilla_firefox_derived__baseline_clients_daily__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=50400)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_analytics_tables__wait_for_bigeye__org_mozilla_firefox_derived__baseline_clients_daily__v1",
            external_dag_id="bqetl_analytics_tables",
            external_task_id="wait_for_bigeye__org_mozilla_firefox_derived__baseline_clients_daily__v1",
        )

        bigeye__org_mozilla_firefox_derived__baseline_clients_daily__v1_external.set_upstream(
            bigeye__org_mozilla_firefox_derived__baseline_clients_daily__v1
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

    with TaskGroup(
        "bigeye__org_mozilla_firefox_derived__baseline_clients_last_seen__v1_external",
        parent_group=task_group_fenix,
    ) as bigeye__org_mozilla_firefox_derived__baseline_clients_last_seen__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_analytics_aggregations__wait_for_bigeye__org_mozilla_firefox_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_analytics_aggregations",
            external_task_id="wait_for_bigeye__org_mozilla_firefox_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=78300)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_analytics_tables__wait_for_bigeye__org_mozilla_firefox_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_analytics_tables",
            external_task_id="wait_for_bigeye__org_mozilla_firefox_derived__baseline_clients_last_seen__v1",
        )

        ExternalTaskMarker(
            task_id="private_bqetl_device_partnerships__wait_for_bigeye__org_mozilla_firefox_derived__baseline_clients_last_seen__v1",
            external_dag_id="private_bqetl_device_partnerships",
            external_task_id="wait_for_bigeye__org_mozilla_firefox_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=57600)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_mobile_kpi_metrics__wait_for_bigeye__org_mozilla_firefox_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_mobile_kpi_metrics",
            external_task_id="wait_for_bigeye__org_mozilla_firefox_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=50400)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_mobile_activation__wait_for_bigeye__org_mozilla_firefox_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_mobile_activation",
            external_task_id="wait_for_bigeye__org_mozilla_firefox_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(seconds=7200)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_marketing_analysis__wait_for_bigeye__org_mozilla_firefox_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_marketing_analysis",
            external_task_id="wait_for_bigeye__org_mozilla_firefox_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=50400)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_fivetran_google_ads__wait_for_bigeye__org_mozilla_firefox_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_fivetran_google_ads",
            external_task_id="wait_for_bigeye__org_mozilla_firefox_derived__baseline_clients_last_seen__v1",
        )

        ExternalTaskMarker(
            task_id="bqetl_search_dashboard__wait_for_bigeye__org_mozilla_firefox_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_search_dashboard",
            external_task_id="wait_for_bigeye__org_mozilla_firefox_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=73800)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_shredder_impact_measurement__wait_for_bigeye__org_mozilla_firefox_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_shredder_impact_measurement",
            external_task_id="wait_for_bigeye__org_mozilla_firefox_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=48000)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_cohort_daily_churn__wait_for_bigeye__org_mozilla_firefox_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_cohort_daily_churn",
            external_task_id="wait_for_bigeye__org_mozilla_firefox_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=40200)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_cohort_retention__wait_for_bigeye__org_mozilla_firefox_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_cohort_retention",
            external_task_id="wait_for_bigeye__org_mozilla_firefox_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=22800)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_nondesktop__wait_for_bigeye__org_mozilla_firefox_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_nondesktop",
            external_task_id="wait_for_bigeye__org_mozilla_firefox_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=82800)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_gud__wait_for_bigeye__org_mozilla_firefox_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_gud",
            external_task_id="wait_for_bigeye__org_mozilla_firefox_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=82800)).isoformat() }}",
        )

        bigeye__org_mozilla_firefox_derived__baseline_clients_last_seen__v1_external.set_upstream(
            bigeye__org_mozilla_firefox_derived__baseline_clients_last_seen__v1
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

    with TaskGroup(
        "bigeye__org_mozilla_ios_fennec_derived__baseline_clients_daily__v1_external",
        parent_group=task_group_firefox_ios,
    ) as bigeye__org_mozilla_ios_fennec_derived__baseline_clients_daily__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_generated_funnels__wait_for_bigeye__org_mozilla_ios_fennec_derived__baseline_clients_daily__v1",
            external_dag_id="bqetl_generated_funnels",
            external_task_id="wait_for_bigeye__org_mozilla_ios_fennec_derived__baseline_clients_daily__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=75600)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_mobile_kpi_metrics__wait_for_bigeye__org_mozilla_ios_fennec_derived__baseline_clients_daily__v1",
            external_dag_id="bqetl_mobile_kpi_metrics",
            external_task_id="wait_for_bigeye__org_mozilla_ios_fennec_derived__baseline_clients_daily__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=50400)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_firefox_ios__wait_for_bigeye__org_mozilla_ios_fennec_derived__baseline_clients_daily__v1",
            external_dag_id="bqetl_firefox_ios",
            external_task_id="wait_for_bigeye__org_mozilla_ios_fennec_derived__baseline_clients_daily__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=79200)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_mobile_feature_usage__wait_for_bigeye__org_mozilla_ios_fennec_derived__baseline_clients_daily__v1",
            external_dag_id="bqetl_mobile_feature_usage",
            external_task_id="wait_for_bigeye__org_mozilla_ios_fennec_derived__baseline_clients_daily__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=50400)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_mobile_activation__wait_for_bigeye__org_mozilla_ios_fennec_derived__baseline_clients_daily__v1",
            external_dag_id="bqetl_mobile_activation",
            external_task_id="wait_for_bigeye__org_mozilla_ios_fennec_derived__baseline_clients_daily__v1",
            execution_date="{{ (execution_date - macros.timedelta(seconds=7200)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_marketing_analysis__wait_for_bigeye__org_mozilla_ios_fennec_derived__baseline_clients_daily__v1",
            external_dag_id="bqetl_marketing_analysis",
            external_task_id="wait_for_bigeye__org_mozilla_ios_fennec_derived__baseline_clients_daily__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=50400)).isoformat() }}",
        )

        bigeye__org_mozilla_ios_fennec_derived__baseline_clients_daily__v1_external.set_upstream(
            bigeye__org_mozilla_ios_fennec_derived__baseline_clients_daily__v1
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

    with TaskGroup(
        "bigeye__org_mozilla_ios_fennec_derived__baseline_clients_last_seen__v1_external",
        parent_group=task_group_firefox_ios,
    ) as bigeye__org_mozilla_ios_fennec_derived__baseline_clients_last_seen__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_analytics_tables__wait_for_bigeye__org_mozilla_ios_fennec_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_analytics_tables",
            external_task_id="wait_for_bigeye__org_mozilla_ios_fennec_derived__baseline_clients_last_seen__v1",
        )

        ExternalTaskMarker(
            task_id="bqetl_generated_funnels__wait_for_bigeye__org_mozilla_ios_fennec_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_generated_funnels",
            external_task_id="wait_for_bigeye__org_mozilla_ios_fennec_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=75600)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_analytics_aggregations__wait_for_bigeye__org_mozilla_ios_fennec_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_analytics_aggregations",
            external_task_id="wait_for_bigeye__org_mozilla_ios_fennec_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=78300)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_firefox_ios__wait_for_bigeye__org_mozilla_ios_fennec_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_firefox_ios",
            external_task_id="wait_for_bigeye__org_mozilla_ios_fennec_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=79200)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_mobile_kpi_metrics__wait_for_bigeye__org_mozilla_ios_fennec_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_mobile_kpi_metrics",
            external_task_id="wait_for_bigeye__org_mozilla_ios_fennec_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=50400)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_mobile_activation__wait_for_bigeye__org_mozilla_ios_fennec_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_mobile_activation",
            external_task_id="wait_for_bigeye__org_mozilla_ios_fennec_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(seconds=7200)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_marketing_analysis__wait_for_bigeye__org_mozilla_ios_fennec_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_marketing_analysis",
            external_task_id="wait_for_bigeye__org_mozilla_ios_fennec_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=50400)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_fivetran_google_ads__wait_for_bigeye__org_mozilla_ios_fennec_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_fivetran_google_ads",
            external_task_id="wait_for_bigeye__org_mozilla_ios_fennec_derived__baseline_clients_last_seen__v1",
        )

        ExternalTaskMarker(
            task_id="bqetl_search_dashboard__wait_for_bigeye__org_mozilla_ios_fennec_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_search_dashboard",
            external_task_id="wait_for_bigeye__org_mozilla_ios_fennec_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=73800)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_shredder_impact_measurement__wait_for_bigeye__org_mozilla_ios_fennec_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_shredder_impact_measurement",
            external_task_id="wait_for_bigeye__org_mozilla_ios_fennec_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=48000)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_cohort_daily_churn__wait_for_bigeye__org_mozilla_ios_fennec_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_cohort_daily_churn",
            external_task_id="wait_for_bigeye__org_mozilla_ios_fennec_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=40200)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_cohort_retention__wait_for_bigeye__org_mozilla_ios_fennec_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_cohort_retention",
            external_task_id="wait_for_bigeye__org_mozilla_ios_fennec_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=22800)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_nondesktop__wait_for_bigeye__org_mozilla_ios_fennec_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_nondesktop",
            external_task_id="wait_for_bigeye__org_mozilla_ios_fennec_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=82800)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_gud__wait_for_bigeye__org_mozilla_ios_fennec_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_gud",
            external_task_id="wait_for_bigeye__org_mozilla_ios_fennec_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=82800)).isoformat() }}",
        )

        bigeye__org_mozilla_ios_fennec_derived__baseline_clients_last_seen__v1_external.set_upstream(
            bigeye__org_mozilla_ios_fennec_derived__baseline_clients_last_seen__v1
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

    with TaskGroup(
        "bigeye__org_mozilla_ios_firefox_derived__baseline_clients_daily__v1_external",
        parent_group=task_group_firefox_ios,
    ) as bigeye__org_mozilla_ios_firefox_derived__baseline_clients_daily__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_generated_funnels__wait_for_bigeye__org_mozilla_ios_firefox_derived__baseline_clients_daily__v1",
            external_dag_id="bqetl_generated_funnels",
            external_task_id="wait_for_bigeye__org_mozilla_ios_firefox_derived__baseline_clients_daily__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=75600)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_mobile_kpi_metrics__wait_for_bigeye__org_mozilla_ios_firefox_derived__baseline_clients_daily__v1",
            external_dag_id="bqetl_mobile_kpi_metrics",
            external_task_id="wait_for_bigeye__org_mozilla_ios_firefox_derived__baseline_clients_daily__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=50400)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_firefox_ios__wait_for_bigeye__org_mozilla_ios_firefox_derived__baseline_clients_daily__v1",
            external_dag_id="bqetl_firefox_ios",
            external_task_id="wait_for_bigeye__org_mozilla_ios_firefox_derived__baseline_clients_daily__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=79200)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_mobile_feature_usage__wait_for_bigeye__org_mozilla_ios_firefox_derived__baseline_clients_daily__v1",
            external_dag_id="bqetl_mobile_feature_usage",
            external_task_id="wait_for_bigeye__org_mozilla_ios_firefox_derived__baseline_clients_daily__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=50400)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_mobile_activation__wait_for_bigeye__org_mozilla_ios_firefox_derived__baseline_clients_daily__v1",
            external_dag_id="bqetl_mobile_activation",
            external_task_id="wait_for_bigeye__org_mozilla_ios_firefox_derived__baseline_clients_daily__v1",
            execution_date="{{ (execution_date - macros.timedelta(seconds=7200)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_marketing_analysis__wait_for_bigeye__org_mozilla_ios_firefox_derived__baseline_clients_daily__v1",
            external_dag_id="bqetl_marketing_analysis",
            external_task_id="wait_for_bigeye__org_mozilla_ios_firefox_derived__baseline_clients_daily__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=50400)).isoformat() }}",
        )

        bigeye__org_mozilla_ios_firefox_derived__baseline_clients_daily__v1_external.set_upstream(
            bigeye__org_mozilla_ios_firefox_derived__baseline_clients_daily__v1
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

    with TaskGroup(
        "bigeye__org_mozilla_ios_firefox_derived__baseline_clients_last_seen__v1_external",
        parent_group=task_group_firefox_ios,
    ) as bigeye__org_mozilla_ios_firefox_derived__baseline_clients_last_seen__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_analytics_tables__wait_for_bigeye__org_mozilla_ios_firefox_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_analytics_tables",
            external_task_id="wait_for_bigeye__org_mozilla_ios_firefox_derived__baseline_clients_last_seen__v1",
        )

        ExternalTaskMarker(
            task_id="bqetl_generated_funnels__wait_for_bigeye__org_mozilla_ios_firefox_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_generated_funnels",
            external_task_id="wait_for_bigeye__org_mozilla_ios_firefox_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=75600)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_analytics_aggregations__wait_for_bigeye__org_mozilla_ios_firefox_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_analytics_aggregations",
            external_task_id="wait_for_bigeye__org_mozilla_ios_firefox_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=78300)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_firefox_ios__wait_for_bigeye__org_mozilla_ios_firefox_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_firefox_ios",
            external_task_id="wait_for_bigeye__org_mozilla_ios_firefox_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=79200)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_mobile_kpi_metrics__wait_for_bigeye__org_mozilla_ios_firefox_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_mobile_kpi_metrics",
            external_task_id="wait_for_bigeye__org_mozilla_ios_firefox_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=50400)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_mobile_activation__wait_for_bigeye__org_mozilla_ios_firefox_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_mobile_activation",
            external_task_id="wait_for_bigeye__org_mozilla_ios_firefox_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(seconds=7200)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_marketing_analysis__wait_for_bigeye__org_mozilla_ios_firefox_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_marketing_analysis",
            external_task_id="wait_for_bigeye__org_mozilla_ios_firefox_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=50400)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_fivetran_google_ads__wait_for_bigeye__org_mozilla_ios_firefox_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_fivetran_google_ads",
            external_task_id="wait_for_bigeye__org_mozilla_ios_firefox_derived__baseline_clients_last_seen__v1",
        )

        ExternalTaskMarker(
            task_id="bqetl_search_dashboard__wait_for_bigeye__org_mozilla_ios_firefox_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_search_dashboard",
            external_task_id="wait_for_bigeye__org_mozilla_ios_firefox_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=73800)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_shredder_impact_measurement__wait_for_bigeye__org_mozilla_ios_firefox_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_shredder_impact_measurement",
            external_task_id="wait_for_bigeye__org_mozilla_ios_firefox_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=48000)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_cohort_daily_churn__wait_for_bigeye__org_mozilla_ios_firefox_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_cohort_daily_churn",
            external_task_id="wait_for_bigeye__org_mozilla_ios_firefox_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=40200)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_cohort_retention__wait_for_bigeye__org_mozilla_ios_firefox_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_cohort_retention",
            external_task_id="wait_for_bigeye__org_mozilla_ios_firefox_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=22800)).isoformat() }}",
        )

        bigeye__org_mozilla_ios_firefox_derived__baseline_clients_last_seen__v1_external.set_upstream(
            bigeye__org_mozilla_ios_firefox_derived__baseline_clients_last_seen__v1
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

    with TaskGroup(
        "bigeye__org_mozilla_ios_firefoxbeta_derived__baseline_clients_daily__v1_external",
        parent_group=task_group_firefox_ios,
    ) as bigeye__org_mozilla_ios_firefoxbeta_derived__baseline_clients_daily__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_generated_funnels__wait_for_bigeye__org_mozilla_ios_firefoxbeta_derived__baseline_clients_daily__v1",
            external_dag_id="bqetl_generated_funnels",
            external_task_id="wait_for_bigeye__org_mozilla_ios_firefoxbeta_derived__baseline_clients_daily__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=75600)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_mobile_kpi_metrics__wait_for_bigeye__org_mozilla_ios_firefoxbeta_derived__baseline_clients_daily__v1",
            external_dag_id="bqetl_mobile_kpi_metrics",
            external_task_id="wait_for_bigeye__org_mozilla_ios_firefoxbeta_derived__baseline_clients_daily__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=50400)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_firefox_ios__wait_for_bigeye__org_mozilla_ios_firefoxbeta_derived__baseline_clients_daily__v1",
            external_dag_id="bqetl_firefox_ios",
            external_task_id="wait_for_bigeye__org_mozilla_ios_firefoxbeta_derived__baseline_clients_daily__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=79200)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_mobile_feature_usage__wait_for_bigeye__org_mozilla_ios_firefoxbeta_derived__baseline_clients_daily__v1",
            external_dag_id="bqetl_mobile_feature_usage",
            external_task_id="wait_for_bigeye__org_mozilla_ios_firefoxbeta_derived__baseline_clients_daily__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=50400)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_mobile_activation__wait_for_bigeye__org_mozilla_ios_firefoxbeta_derived__baseline_clients_daily__v1",
            external_dag_id="bqetl_mobile_activation",
            external_task_id="wait_for_bigeye__org_mozilla_ios_firefoxbeta_derived__baseline_clients_daily__v1",
            execution_date="{{ (execution_date - macros.timedelta(seconds=7200)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_marketing_analysis__wait_for_bigeye__org_mozilla_ios_firefoxbeta_derived__baseline_clients_daily__v1",
            external_dag_id="bqetl_marketing_analysis",
            external_task_id="wait_for_bigeye__org_mozilla_ios_firefoxbeta_derived__baseline_clients_daily__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=50400)).isoformat() }}",
        )

        bigeye__org_mozilla_ios_firefoxbeta_derived__baseline_clients_daily__v1_external.set_upstream(
            bigeye__org_mozilla_ios_firefoxbeta_derived__baseline_clients_daily__v1
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

    with TaskGroup(
        "bigeye__org_mozilla_ios_firefoxbeta_derived__baseline_clients_last_seen__v1_external",
        parent_group=task_group_firefox_ios,
    ) as bigeye__org_mozilla_ios_firefoxbeta_derived__baseline_clients_last_seen__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_analytics_tables__wait_for_bigeye__org_mozilla_ios_firefoxbeta_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_analytics_tables",
            external_task_id="wait_for_bigeye__org_mozilla_ios_firefoxbeta_derived__baseline_clients_last_seen__v1",
        )

        ExternalTaskMarker(
            task_id="bqetl_generated_funnels__wait_for_bigeye__org_mozilla_ios_firefoxbeta_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_generated_funnels",
            external_task_id="wait_for_bigeye__org_mozilla_ios_firefoxbeta_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=75600)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_analytics_aggregations__wait_for_bigeye__org_mozilla_ios_firefoxbeta_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_analytics_aggregations",
            external_task_id="wait_for_bigeye__org_mozilla_ios_firefoxbeta_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=78300)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_firefox_ios__wait_for_bigeye__org_mozilla_ios_firefoxbeta_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_firefox_ios",
            external_task_id="wait_for_bigeye__org_mozilla_ios_firefoxbeta_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=79200)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_mobile_kpi_metrics__wait_for_bigeye__org_mozilla_ios_firefoxbeta_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_mobile_kpi_metrics",
            external_task_id="wait_for_bigeye__org_mozilla_ios_firefoxbeta_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=50400)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_mobile_activation__wait_for_bigeye__org_mozilla_ios_firefoxbeta_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_mobile_activation",
            external_task_id="wait_for_bigeye__org_mozilla_ios_firefoxbeta_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(seconds=7200)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_marketing_analysis__wait_for_bigeye__org_mozilla_ios_firefoxbeta_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_marketing_analysis",
            external_task_id="wait_for_bigeye__org_mozilla_ios_firefoxbeta_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=50400)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_fivetran_google_ads__wait_for_bigeye__org_mozilla_ios_firefoxbeta_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_fivetran_google_ads",
            external_task_id="wait_for_bigeye__org_mozilla_ios_firefoxbeta_derived__baseline_clients_last_seen__v1",
        )

        ExternalTaskMarker(
            task_id="bqetl_search_dashboard__wait_for_bigeye__org_mozilla_ios_firefoxbeta_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_search_dashboard",
            external_task_id="wait_for_bigeye__org_mozilla_ios_firefoxbeta_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=73800)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_shredder_impact_measurement__wait_for_bigeye__org_mozilla_ios_firefoxbeta_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_shredder_impact_measurement",
            external_task_id="wait_for_bigeye__org_mozilla_ios_firefoxbeta_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=48000)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_cohort_daily_churn__wait_for_bigeye__org_mozilla_ios_firefoxbeta_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_cohort_daily_churn",
            external_task_id="wait_for_bigeye__org_mozilla_ios_firefoxbeta_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=40200)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_cohort_retention__wait_for_bigeye__org_mozilla_ios_firefoxbeta_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_cohort_retention",
            external_task_id="wait_for_bigeye__org_mozilla_ios_firefoxbeta_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=22800)).isoformat() }}",
        )

        bigeye__org_mozilla_ios_firefoxbeta_derived__baseline_clients_last_seen__v1_external.set_upstream(
            bigeye__org_mozilla_ios_firefoxbeta_derived__baseline_clients_last_seen__v1
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

    with TaskGroup(
        "bigeye__org_mozilla_ios_focus_derived__baseline_clients_last_seen__v1_external",
        parent_group=task_group_focus_ios,
    ) as bigeye__org_mozilla_ios_focus_derived__baseline_clients_last_seen__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_analytics_tables__wait_for_bigeye__org_mozilla_ios_focus_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_analytics_tables",
            external_task_id="wait_for_bigeye__org_mozilla_ios_focus_derived__baseline_clients_last_seen__v1",
        )

        ExternalTaskMarker(
            task_id="bqetl_analytics_aggregations__wait_for_bigeye__org_mozilla_ios_focus_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_analytics_aggregations",
            external_task_id="wait_for_bigeye__org_mozilla_ios_focus_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=78300)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_mobile_kpi_metrics__wait_for_bigeye__org_mozilla_ios_focus_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_mobile_kpi_metrics",
            external_task_id="wait_for_bigeye__org_mozilla_ios_focus_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=50400)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_fivetran_google_ads__wait_for_bigeye__org_mozilla_ios_focus_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_fivetran_google_ads",
            external_task_id="wait_for_bigeye__org_mozilla_ios_focus_derived__baseline_clients_last_seen__v1",
        )

        ExternalTaskMarker(
            task_id="bqetl_search_dashboard__wait_for_bigeye__org_mozilla_ios_focus_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_search_dashboard",
            external_task_id="wait_for_bigeye__org_mozilla_ios_focus_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=73800)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_shredder_impact_measurement__wait_for_bigeye__org_mozilla_ios_focus_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_shredder_impact_measurement",
            external_task_id="wait_for_bigeye__org_mozilla_ios_focus_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=48000)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_cohort_daily_churn__wait_for_bigeye__org_mozilla_ios_focus_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_cohort_daily_churn",
            external_task_id="wait_for_bigeye__org_mozilla_ios_focus_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=40200)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_cohort_retention__wait_for_bigeye__org_mozilla_ios_focus_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_cohort_retention",
            external_task_id="wait_for_bigeye__org_mozilla_ios_focus_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=22800)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_nondesktop__wait_for_bigeye__org_mozilla_ios_focus_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_nondesktop",
            external_task_id="wait_for_bigeye__org_mozilla_ios_focus_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=82800)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_gud__wait_for_bigeye__org_mozilla_ios_focus_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_gud",
            external_task_id="wait_for_bigeye__org_mozilla_ios_focus_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=82800)).isoformat() }}",
        )

        bigeye__org_mozilla_ios_focus_derived__baseline_clients_last_seen__v1_external.set_upstream(
            bigeye__org_mozilla_ios_focus_derived__baseline_clients_last_seen__v1
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

    checks__fail_experimenter_cirrus_derived__baseline_clients_last_seen__v1 = bigquery_dq_check(
        task_id="checks__fail_experimenter_cirrus_derived__baseline_clients_last_seen__v1",
        source_table="baseline_clients_last_seen_v1",
        dataset_id="experimenter_cirrus_derived",
        project_id="moz-fx-data-shared-prod",
        is_dq_check_fail=True,
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        parameters=["submission_date:DATE:{{ds}}"],
        retries=0,
        task_group=task_group_experimenter_cirrus,
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

    with TaskGroup(
        "checks__fail_org_mozilla_focus_beta_derived__baseline_clients_last_seen__v1_external",
        parent_group=task_group_focus_android,
    ) as checks__fail_org_mozilla_focus_beta_derived__baseline_clients_last_seen__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_analytics_tables__wait_for_checks__fail_org_mozilla_focus_beta_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_analytics_tables",
            external_task_id="wait_for_checks__fail_org_mozilla_focus_beta_derived__baseline_clients_last_seen__v1",
        )

        ExternalTaskMarker(
            task_id="bqetl_analytics_aggregations__wait_for_checks__fail_org_mozilla_focus_beta_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_analytics_aggregations",
            external_task_id="wait_for_checks__fail_org_mozilla_focus_beta_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=78300)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_mobile_kpi_metrics__wait_for_checks__fail_org_mozilla_focus_beta_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_mobile_kpi_metrics",
            external_task_id="wait_for_checks__fail_org_mozilla_focus_beta_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=50400)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_fivetran_google_ads__wait_for_checks__fail_org_mozilla_focus_beta_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_fivetran_google_ads",
            external_task_id="wait_for_checks__fail_org_mozilla_focus_beta_derived__baseline_clients_last_seen__v1",
        )

        ExternalTaskMarker(
            task_id="bqetl_search_dashboard__wait_for_checks__fail_org_mozilla_focus_beta_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_search_dashboard",
            external_task_id="wait_for_checks__fail_org_mozilla_focus_beta_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=73800)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_shredder_impact_measurement__wait_for_checks__fail_org_mozilla_focus_beta_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_shredder_impact_measurement",
            external_task_id="wait_for_checks__fail_org_mozilla_focus_beta_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=48000)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_cohort_daily_churn__wait_for_checks__fail_org_mozilla_focus_beta_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_cohort_daily_churn",
            external_task_id="wait_for_checks__fail_org_mozilla_focus_beta_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=40200)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_cohort_retention__wait_for_checks__fail_org_mozilla_focus_beta_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_cohort_retention",
            external_task_id="wait_for_checks__fail_org_mozilla_focus_beta_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=22800)).isoformat() }}",
        )

        checks__fail_org_mozilla_focus_beta_derived__baseline_clients_last_seen__v1_external.set_upstream(
            checks__fail_org_mozilla_focus_beta_derived__baseline_clients_last_seen__v1
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

    with TaskGroup(
        "checks__fail_org_mozilla_focus_derived__baseline_clients_last_seen__v1_external",
        parent_group=task_group_focus_android,
    ) as checks__fail_org_mozilla_focus_derived__baseline_clients_last_seen__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_analytics_tables__wait_for_checks__fail_org_mozilla_focus_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_analytics_tables",
            external_task_id="wait_for_checks__fail_org_mozilla_focus_derived__baseline_clients_last_seen__v1",
        )

        ExternalTaskMarker(
            task_id="bqetl_analytics_aggregations__wait_for_checks__fail_org_mozilla_focus_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_analytics_aggregations",
            external_task_id="wait_for_checks__fail_org_mozilla_focus_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=78300)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_mobile_kpi_metrics__wait_for_checks__fail_org_mozilla_focus_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_mobile_kpi_metrics",
            external_task_id="wait_for_checks__fail_org_mozilla_focus_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=50400)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_fivetran_google_ads__wait_for_checks__fail_org_mozilla_focus_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_fivetran_google_ads",
            external_task_id="wait_for_checks__fail_org_mozilla_focus_derived__baseline_clients_last_seen__v1",
        )

        ExternalTaskMarker(
            task_id="bqetl_search_dashboard__wait_for_checks__fail_org_mozilla_focus_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_search_dashboard",
            external_task_id="wait_for_checks__fail_org_mozilla_focus_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=73800)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_shredder_impact_measurement__wait_for_checks__fail_org_mozilla_focus_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_shredder_impact_measurement",
            external_task_id="wait_for_checks__fail_org_mozilla_focus_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=48000)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_cohort_daily_churn__wait_for_checks__fail_org_mozilla_focus_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_cohort_daily_churn",
            external_task_id="wait_for_checks__fail_org_mozilla_focus_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=40200)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_cohort_retention__wait_for_checks__fail_org_mozilla_focus_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_cohort_retention",
            external_task_id="wait_for_checks__fail_org_mozilla_focus_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=22800)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_nondesktop__wait_for_checks__fail_org_mozilla_focus_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_nondesktop",
            external_task_id="wait_for_checks__fail_org_mozilla_focus_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=82800)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_gud__wait_for_checks__fail_org_mozilla_focus_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_gud",
            external_task_id="wait_for_checks__fail_org_mozilla_focus_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=82800)).isoformat() }}",
        )

        checks__fail_org_mozilla_focus_derived__baseline_clients_last_seen__v1_external.set_upstream(
            checks__fail_org_mozilla_focus_derived__baseline_clients_last_seen__v1
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

    with TaskGroup(
        "checks__fail_org_mozilla_focus_nightly_derived__baseline_clients_last_seen__v1_external",
        parent_group=task_group_focus_android,
    ) as checks__fail_org_mozilla_focus_nightly_derived__baseline_clients_last_seen__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_analytics_tables__wait_for_checks__fail_org_mozilla_focus_nightly_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_analytics_tables",
            external_task_id="wait_for_checks__fail_org_mozilla_focus_nightly_derived__baseline_clients_last_seen__v1",
        )

        ExternalTaskMarker(
            task_id="bqetl_analytics_aggregations__wait_for_checks__fail_org_mozilla_focus_nightly_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_analytics_aggregations",
            external_task_id="wait_for_checks__fail_org_mozilla_focus_nightly_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=78300)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_mobile_kpi_metrics__wait_for_checks__fail_org_mozilla_focus_nightly_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_mobile_kpi_metrics",
            external_task_id="wait_for_checks__fail_org_mozilla_focus_nightly_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=50400)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_fivetran_google_ads__wait_for_checks__fail_org_mozilla_focus_nightly_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_fivetran_google_ads",
            external_task_id="wait_for_checks__fail_org_mozilla_focus_nightly_derived__baseline_clients_last_seen__v1",
        )

        ExternalTaskMarker(
            task_id="bqetl_search_dashboard__wait_for_checks__fail_org_mozilla_focus_nightly_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_search_dashboard",
            external_task_id="wait_for_checks__fail_org_mozilla_focus_nightly_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=73800)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_shredder_impact_measurement__wait_for_checks__fail_org_mozilla_focus_nightly_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_shredder_impact_measurement",
            external_task_id="wait_for_checks__fail_org_mozilla_focus_nightly_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=48000)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_cohort_daily_churn__wait_for_checks__fail_org_mozilla_focus_nightly_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_cohort_daily_churn",
            external_task_id="wait_for_checks__fail_org_mozilla_focus_nightly_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=40200)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_cohort_retention__wait_for_checks__fail_org_mozilla_focus_nightly_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_cohort_retention",
            external_task_id="wait_for_checks__fail_org_mozilla_focus_nightly_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=22800)).isoformat() }}",
        )

        checks__fail_org_mozilla_focus_nightly_derived__baseline_clients_last_seen__v1_external.set_upstream(
            checks__fail_org_mozilla_focus_nightly_derived__baseline_clients_last_seen__v1
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

    with TaskGroup(
        "checks__fail_org_mozilla_ios_klar_derived__baseline_clients_last_seen__v1_external",
        parent_group=task_group_klar_ios,
    ) as checks__fail_org_mozilla_ios_klar_derived__baseline_clients_last_seen__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_analytics_tables__wait_for_checks__fail_org_mozilla_ios_klar_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_analytics_tables",
            external_task_id="wait_for_checks__fail_org_mozilla_ios_klar_derived__baseline_clients_last_seen__v1",
        )

        ExternalTaskMarker(
            task_id="bqetl_fivetran_google_ads__wait_for_checks__fail_org_mozilla_ios_klar_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_fivetran_google_ads",
            external_task_id="wait_for_checks__fail_org_mozilla_ios_klar_derived__baseline_clients_last_seen__v1",
        )

        ExternalTaskMarker(
            task_id="bqetl_analytics_aggregations__wait_for_checks__fail_org_mozilla_ios_klar_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_analytics_aggregations",
            external_task_id="wait_for_checks__fail_org_mozilla_ios_klar_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=78300)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_mobile_kpi_metrics__wait_for_checks__fail_org_mozilla_ios_klar_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_mobile_kpi_metrics",
            external_task_id="wait_for_checks__fail_org_mozilla_ios_klar_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=50400)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_search_dashboard__wait_for_checks__fail_org_mozilla_ios_klar_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_search_dashboard",
            external_task_id="wait_for_checks__fail_org_mozilla_ios_klar_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=73800)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_shredder_impact_measurement__wait_for_checks__fail_org_mozilla_ios_klar_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_shredder_impact_measurement",
            external_task_id="wait_for_checks__fail_org_mozilla_ios_klar_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=48000)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_cohort_daily_churn__wait_for_checks__fail_org_mozilla_ios_klar_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_cohort_daily_churn",
            external_task_id="wait_for_checks__fail_org_mozilla_ios_klar_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=40200)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_cohort_retention__wait_for_checks__fail_org_mozilla_ios_klar_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_cohort_retention",
            external_task_id="wait_for_checks__fail_org_mozilla_ios_klar_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=22800)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_nondesktop__wait_for_checks__fail_org_mozilla_ios_klar_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_nondesktop",
            external_task_id="wait_for_checks__fail_org_mozilla_ios_klar_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=82800)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_gud__wait_for_checks__fail_org_mozilla_ios_klar_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_gud",
            external_task_id="wait_for_checks__fail_org_mozilla_ios_klar_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=82800)).isoformat() }}",
        )

        checks__fail_org_mozilla_ios_klar_derived__baseline_clients_last_seen__v1_external.set_upstream(
            checks__fail_org_mozilla_ios_klar_derived__baseline_clients_last_seen__v1
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

    with TaskGroup(
        "checks__fail_org_mozilla_ios_lockbox_derived__baseline_clients_last_seen__v1_external",
        parent_group=task_group_lockwise_ios,
    ) as checks__fail_org_mozilla_ios_lockbox_derived__baseline_clients_last_seen__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_nondesktop__wait_for_checks__fail_org_mozilla_ios_lockbox_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_nondesktop",
            external_task_id="wait_for_checks__fail_org_mozilla_ios_lockbox_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=82800)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_gud__wait_for_checks__fail_org_mozilla_ios_lockbox_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_gud",
            external_task_id="wait_for_checks__fail_org_mozilla_ios_lockbox_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=82800)).isoformat() }}",
        )

        checks__fail_org_mozilla_ios_lockbox_derived__baseline_clients_last_seen__v1_external.set_upstream(
            checks__fail_org_mozilla_ios_lockbox_derived__baseline_clients_last_seen__v1
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

    with TaskGroup(
        "checks__fail_org_mozilla_klar_derived__baseline_clients_last_seen__v1_external",
        parent_group=task_group_klar_android,
    ) as checks__fail_org_mozilla_klar_derived__baseline_clients_last_seen__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_analytics_tables__wait_for_checks__fail_org_mozilla_klar_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_analytics_tables",
            external_task_id="wait_for_checks__fail_org_mozilla_klar_derived__baseline_clients_last_seen__v1",
        )

        ExternalTaskMarker(
            task_id="bqetl_fivetran_google_ads__wait_for_checks__fail_org_mozilla_klar_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_fivetran_google_ads",
            external_task_id="wait_for_checks__fail_org_mozilla_klar_derived__baseline_clients_last_seen__v1",
        )

        ExternalTaskMarker(
            task_id="bqetl_analytics_aggregations__wait_for_checks__fail_org_mozilla_klar_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_analytics_aggregations",
            external_task_id="wait_for_checks__fail_org_mozilla_klar_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=78300)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_mobile_kpi_metrics__wait_for_checks__fail_org_mozilla_klar_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_mobile_kpi_metrics",
            external_task_id="wait_for_checks__fail_org_mozilla_klar_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=50400)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_search_dashboard__wait_for_checks__fail_org_mozilla_klar_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_search_dashboard",
            external_task_id="wait_for_checks__fail_org_mozilla_klar_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=73800)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_shredder_impact_measurement__wait_for_checks__fail_org_mozilla_klar_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_shredder_impact_measurement",
            external_task_id="wait_for_checks__fail_org_mozilla_klar_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=48000)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_cohort_daily_churn__wait_for_checks__fail_org_mozilla_klar_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_cohort_daily_churn",
            external_task_id="wait_for_checks__fail_org_mozilla_klar_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=40200)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_cohort_retention__wait_for_checks__fail_org_mozilla_klar_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_cohort_retention",
            external_task_id="wait_for_checks__fail_org_mozilla_klar_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=22800)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_nondesktop__wait_for_checks__fail_org_mozilla_klar_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_nondesktop",
            external_task_id="wait_for_checks__fail_org_mozilla_klar_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=82800)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_gud__wait_for_checks__fail_org_mozilla_klar_derived__baseline_clients_last_seen__v1",
            external_dag_id="bqetl_gud",
            external_task_id="wait_for_checks__fail_org_mozilla_klar_derived__baseline_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=82800)).isoformat() }}",
        )

        checks__fail_org_mozilla_klar_derived__baseline_clients_last_seen__v1_external.set_upstream(
            checks__fail_org_mozilla_klar_derived__baseline_clients_last_seen__v1
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

    experimenter_cirrus_derived__baseline_clients_daily__v1 = bigquery_etl_query(
        task_id="experimenter_cirrus_derived__baseline_clients_daily__v1",
        destination_table="baseline_clients_daily_v1",
        dataset_id="experimenter_cirrus_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_experimenter_cirrus,
    )

    experimenter_cirrus_derived__baseline_clients_first_seen__v1 = bigquery_etl_query(
        task_id="experimenter_cirrus_derived__baseline_clients_first_seen__v1",
        destination_table="baseline_clients_first_seen_v1",
        dataset_id="experimenter_cirrus_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=True,
        parameters=["submission_date:DATE:{{ds}}"],
        task_group=task_group_experimenter_cirrus,
    )

    experimenter_cirrus_derived__baseline_clients_last_seen__v1 = bigquery_etl_query(
        task_id="experimenter_cirrus_derived__baseline_clients_last_seen__v1",
        destination_table="baseline_clients_last_seen_v1",
        dataset_id="experimenter_cirrus_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=True,
        task_group=task_group_experimenter_cirrus,
    )

    experimenter_cirrus_derived__events_stream__v1 = bigquery_etl_query(
        task_id="experimenter_cirrus_derived__events_stream__v1",
        destination_table="events_stream_v1",
        dataset_id="experimenter_cirrus_derived",
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
        task_group=task_group_experimenter_cirrus,
    )

    experimenter_cirrus_derived__metrics_clients_daily__v1 = bigquery_etl_query(
        task_id="experimenter_cirrus_derived__metrics_clients_daily__v1",
        destination_table="metrics_clients_daily_v1",
        dataset_id="experimenter_cirrus_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        task_group=task_group_experimenter_cirrus,
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

    firefox_desktop_background_update_derived__events_stream__v1 = GKEPodOperator(
        task_id="firefox_desktop_background_update_derived__events_stream__v1",
        arguments=[
            "python",
            "sql/moz-fx-data-shared-prod/firefox_desktop_background_update_derived/events_stream_v1/query.py",
        ]
        + [
            "--billing-project",
            "moz-fx-data-backfill-2",
            "--slices",
            "10",
            "--submission-date",
            "{{ ds }}",
            "--destination-table",
            "moz-fx-data-shared-prod.firefox_desktop_background_update_derived.events_stream_v1",
        ],
        image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
        owner="jrediger@mozilla.com",
        email=[
            "ascholtz@mozilla.com",
            "jrediger@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wstuckey@mozilla.com",
        ],
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

    firefox_desktop_derived__events_stream__v1 = GKEPodOperator(
        task_id="firefox_desktop_derived__events_stream__v1",
        arguments=[
            "python",
            "sql/moz-fx-data-shared-prod/firefox_desktop_derived/events_stream_v1/query.py",
        ]
        + [
            "--billing-project",
            "moz-fx-data-backfill-2",
            "--slices",
            "10",
            "--submission-date",
            "{{ ds }}",
            "--destination-table",
            "moz-fx-data-shared-prod.firefox_desktop_derived.events_stream_v1",
        ],
        image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
        owner="jrediger@mozilla.com",
        email=[
            "ascholtz@mozilla.com",
            "jrediger@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wstuckey@mozilla.com",
        ],
        task_group=task_group_firefox_desktop,
    )

    with TaskGroup(
        "firefox_desktop_derived__events_stream__v1_external",
        parent_group=task_group_firefox_desktop,
    ) as firefox_desktop_derived__events_stream__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_experiments_daily__wait_for_firefox_desktop_derived__events_stream__v1",
            external_dag_id="bqetl_experiments_daily",
            external_task_id="wait_for_firefox_desktop_derived__events_stream__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=82800)).isoformat() }}",
        )

        firefox_desktop_derived__events_stream__v1_external.set_upstream(
            firefox_desktop_derived__events_stream__v1
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

    with TaskGroup(
        "focus_android_derived__clients_last_seen_joined__v1_external",
        parent_group=task_group_focus_android,
    ) as focus_android_derived__clients_last_seen_joined__v1_external:
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

    with TaskGroup(
        "focus_android_derived__metrics_clients_last_seen__v1_external",
        parent_group=task_group_focus_android,
    ) as focus_android_derived__metrics_clients_last_seen__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_analytics_aggregations__wait_for_focus_android_derived__metrics_clients_last_seen__v1",
            external_dag_id="bqetl_analytics_aggregations",
            external_task_id="wait_for_focus_android_derived__metrics_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=78300)).isoformat() }}",
        )

        focus_android_derived__metrics_clients_last_seen__v1_external.set_upstream(
            focus_android_derived__metrics_clients_last_seen__v1
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

    with TaskGroup(
        "focus_ios_derived__metrics_clients_last_seen__v1_external",
        parent_group=task_group_focus_ios,
    ) as focus_ios_derived__metrics_clients_last_seen__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_analytics_aggregations__wait_for_focus_ios_derived__metrics_clients_last_seen__v1",
            external_dag_id="bqetl_analytics_aggregations",
            external_task_id="wait_for_focus_ios_derived__metrics_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=78300)).isoformat() }}",
        )

        focus_ios_derived__metrics_clients_last_seen__v1_external.set_upstream(
            focus_ios_derived__metrics_clients_last_seen__v1
        )

    glam_derived__events_stream__v1 = bigquery_etl_query(
        task_id="glam_derived__events_stream__v1",
        destination_table="events_stream_v1",
        dataset_id="glam_derived",
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
        task_group=task_group_glam,
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

    with TaskGroup(
        "klar_android_derived__metrics_clients_last_seen__v1_external",
        parent_group=task_group_klar_android,
    ) as klar_android_derived__metrics_clients_last_seen__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_analytics_aggregations__wait_for_klar_android_derived__metrics_clients_last_seen__v1",
            external_dag_id="bqetl_analytics_aggregations",
            external_task_id="wait_for_klar_android_derived__metrics_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=78300)).isoformat() }}",
        )

        klar_android_derived__metrics_clients_last_seen__v1_external.set_upstream(
            klar_android_derived__metrics_clients_last_seen__v1
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

    with TaskGroup(
        "klar_ios_derived__metrics_clients_last_seen__v1_external",
        parent_group=task_group_klar_ios,
    ) as klar_ios_derived__metrics_clients_last_seen__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_analytics_aggregations__wait_for_klar_ios_derived__metrics_clients_last_seen__v1",
            external_dag_id="bqetl_analytics_aggregations",
            external_task_id="wait_for_klar_ios_derived__metrics_clients_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=78300)).isoformat() }}",
        )

        klar_ios_derived__metrics_clients_last_seen__v1_external.set_upstream(
            klar_ios_derived__metrics_clients_last_seen__v1
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

    mdn_fred_derived__events_stream__v1 = bigquery_etl_query(
        task_id="mdn_fred_derived__events_stream__v1",
        destination_table="events_stream_v1",
        dataset_id="mdn_fred_derived",
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
        task_group=task_group_mdn_fred,
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

    with TaskGroup(
        "org_mozilla_fenix_derived__events_stream__v1_external",
        parent_group=task_group_fenix,
    ) as org_mozilla_fenix_derived__events_stream__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_generated_funnels__wait_for_org_mozilla_fenix_derived__events_stream__v1",
            external_dag_id="bqetl_generated_funnels",
            external_task_id="wait_for_org_mozilla_fenix_derived__events_stream__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=75600)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_experiments_daily__wait_for_org_mozilla_fenix_derived__events_stream__v1",
            external_dag_id="bqetl_experiments_daily",
            external_task_id="wait_for_org_mozilla_fenix_derived__events_stream__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=82800)).isoformat() }}",
        )

        org_mozilla_fenix_derived__events_stream__v1_external.set_upstream(
            org_mozilla_fenix_derived__events_stream__v1
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

    with TaskGroup(
        "org_mozilla_fenix_nightly_derived__events_stream__v1_external",
        parent_group=task_group_fenix,
    ) as org_mozilla_fenix_nightly_derived__events_stream__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_generated_funnels__wait_for_org_mozilla_fenix_nightly_derived__events_stream__v1",
            external_dag_id="bqetl_generated_funnels",
            external_task_id="wait_for_org_mozilla_fenix_nightly_derived__events_stream__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=75600)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_experiments_daily__wait_for_org_mozilla_fenix_nightly_derived__events_stream__v1",
            external_dag_id="bqetl_experiments_daily",
            external_task_id="wait_for_org_mozilla_fenix_nightly_derived__events_stream__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=82800)).isoformat() }}",
        )

        org_mozilla_fenix_nightly_derived__events_stream__v1_external.set_upstream(
            org_mozilla_fenix_nightly_derived__events_stream__v1
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

    with TaskGroup(
        "org_mozilla_fennec_aurora_derived__events_stream__v1_external",
        parent_group=task_group_fenix,
    ) as org_mozilla_fennec_aurora_derived__events_stream__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_generated_funnels__wait_for_org_mozilla_fennec_aurora_derived__events_stream__v1",
            external_dag_id="bqetl_generated_funnels",
            external_task_id="wait_for_org_mozilla_fennec_aurora_derived__events_stream__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=75600)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_experiments_daily__wait_for_org_mozilla_fennec_aurora_derived__events_stream__v1",
            external_dag_id="bqetl_experiments_daily",
            external_task_id="wait_for_org_mozilla_fennec_aurora_derived__events_stream__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=82800)).isoformat() }}",
        )

        org_mozilla_fennec_aurora_derived__events_stream__v1_external.set_upstream(
            org_mozilla_fennec_aurora_derived__events_stream__v1
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

    with TaskGroup(
        "org_mozilla_firefox_beta_derived__events_stream__v1_external",
        parent_group=task_group_fenix,
    ) as org_mozilla_firefox_beta_derived__events_stream__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_generated_funnels__wait_for_org_mozilla_firefox_beta_derived__events_stream__v1",
            external_dag_id="bqetl_generated_funnels",
            external_task_id="wait_for_org_mozilla_firefox_beta_derived__events_stream__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=75600)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_experiments_daily__wait_for_org_mozilla_firefox_beta_derived__events_stream__v1",
            external_dag_id="bqetl_experiments_daily",
            external_task_id="wait_for_org_mozilla_firefox_beta_derived__events_stream__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=82800)).isoformat() }}",
        )

        org_mozilla_firefox_beta_derived__events_stream__v1_external.set_upstream(
            org_mozilla_firefox_beta_derived__events_stream__v1
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

    with TaskGroup(
        "org_mozilla_firefox_derived__events_stream__v1_external",
        parent_group=task_group_fenix,
    ) as org_mozilla_firefox_derived__events_stream__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_generated_funnels__wait_for_org_mozilla_firefox_derived__events_stream__v1",
            external_dag_id="bqetl_generated_funnels",
            external_task_id="wait_for_org_mozilla_firefox_derived__events_stream__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=75600)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_experiments_daily__wait_for_org_mozilla_firefox_derived__events_stream__v1",
            external_dag_id="bqetl_experiments_daily",
            external_task_id="wait_for_org_mozilla_firefox_derived__events_stream__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=82800)).isoformat() }}",
        )

        org_mozilla_firefox_derived__events_stream__v1_external.set_upstream(
            org_mozilla_firefox_derived__events_stream__v1
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

    with TaskGroup(
        "org_mozilla_focus_beta_derived__baseline_clients_daily__v1_external",
        parent_group=task_group_focus_android,
    ) as org_mozilla_focus_beta_derived__baseline_clients_daily__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_mobile_kpi_metrics__wait_for_org_mozilla_focus_beta_derived__baseline_clients_daily__v1",
            external_dag_id="bqetl_mobile_kpi_metrics",
            external_task_id="wait_for_org_mozilla_focus_beta_derived__baseline_clients_daily__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=50400)).isoformat() }}",
        )

        org_mozilla_focus_beta_derived__baseline_clients_daily__v1_external.set_upstream(
            org_mozilla_focus_beta_derived__baseline_clients_daily__v1
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

    with TaskGroup(
        "org_mozilla_focus_beta_derived__events_stream__v1_external",
        parent_group=task_group_focus_android,
    ) as org_mozilla_focus_beta_derived__events_stream__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_experiments_daily__wait_for_org_mozilla_focus_beta_derived__events_stream__v1",
            external_dag_id="bqetl_experiments_daily",
            external_task_id="wait_for_org_mozilla_focus_beta_derived__events_stream__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=82800)).isoformat() }}",
        )

        org_mozilla_focus_beta_derived__events_stream__v1_external.set_upstream(
            org_mozilla_focus_beta_derived__events_stream__v1
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

    with TaskGroup(
        "org_mozilla_focus_derived__baseline_clients_daily__v1_external",
        parent_group=task_group_focus_android,
    ) as org_mozilla_focus_derived__baseline_clients_daily__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_mobile_kpi_metrics__wait_for_org_mozilla_focus_derived__baseline_clients_daily__v1",
            external_dag_id="bqetl_mobile_kpi_metrics",
            external_task_id="wait_for_org_mozilla_focus_derived__baseline_clients_daily__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=50400)).isoformat() }}",
        )

        org_mozilla_focus_derived__baseline_clients_daily__v1_external.set_upstream(
            org_mozilla_focus_derived__baseline_clients_daily__v1
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

    with TaskGroup(
        "org_mozilla_focus_derived__events_stream__v1_external",
        parent_group=task_group_focus_android,
    ) as org_mozilla_focus_derived__events_stream__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_experiments_daily__wait_for_org_mozilla_focus_derived__events_stream__v1",
            external_dag_id="bqetl_experiments_daily",
            external_task_id="wait_for_org_mozilla_focus_derived__events_stream__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=82800)).isoformat() }}",
        )

        org_mozilla_focus_derived__events_stream__v1_external.set_upstream(
            org_mozilla_focus_derived__events_stream__v1
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

    with TaskGroup(
        "org_mozilla_focus_nightly_derived__baseline_clients_daily__v1_external",
        parent_group=task_group_focus_android,
    ) as org_mozilla_focus_nightly_derived__baseline_clients_daily__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_mobile_kpi_metrics__wait_for_org_mozilla_focus_nightly_derived__baseline_clients_daily__v1",
            external_dag_id="bqetl_mobile_kpi_metrics",
            external_task_id="wait_for_org_mozilla_focus_nightly_derived__baseline_clients_daily__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=50400)).isoformat() }}",
        )

        org_mozilla_focus_nightly_derived__baseline_clients_daily__v1_external.set_upstream(
            org_mozilla_focus_nightly_derived__baseline_clients_daily__v1
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

    with TaskGroup(
        "org_mozilla_focus_nightly_derived__events_stream__v1_external",
        parent_group=task_group_focus_android,
    ) as org_mozilla_focus_nightly_derived__events_stream__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_experiments_daily__wait_for_org_mozilla_focus_nightly_derived__events_stream__v1",
            external_dag_id="bqetl_experiments_daily",
            external_task_id="wait_for_org_mozilla_focus_nightly_derived__events_stream__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=82800)).isoformat() }}",
        )

        org_mozilla_focus_nightly_derived__events_stream__v1_external.set_upstream(
            org_mozilla_focus_nightly_derived__events_stream__v1
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

    with TaskGroup(
        "org_mozilla_ios_fennec_derived__events_stream__v1_external",
        parent_group=task_group_firefox_ios,
    ) as org_mozilla_ios_fennec_derived__events_stream__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_generated_funnels__wait_for_org_mozilla_ios_fennec_derived__events_stream__v1",
            external_dag_id="bqetl_generated_funnels",
            external_task_id="wait_for_org_mozilla_ios_fennec_derived__events_stream__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=75600)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_experiments_daily__wait_for_org_mozilla_ios_fennec_derived__events_stream__v1",
            external_dag_id="bqetl_experiments_daily",
            external_task_id="wait_for_org_mozilla_ios_fennec_derived__events_stream__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=82800)).isoformat() }}",
        )

        org_mozilla_ios_fennec_derived__events_stream__v1_external.set_upstream(
            org_mozilla_ios_fennec_derived__events_stream__v1
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

    with TaskGroup(
        "org_mozilla_ios_firefox_derived__events_stream__v1_external",
        parent_group=task_group_firefox_ios,
    ) as org_mozilla_ios_firefox_derived__events_stream__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_generated_funnels__wait_for_org_mozilla_ios_firefox_derived__events_stream__v1",
            external_dag_id="bqetl_generated_funnels",
            external_task_id="wait_for_org_mozilla_ios_firefox_derived__events_stream__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=75600)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_experiments_daily__wait_for_org_mozilla_ios_firefox_derived__events_stream__v1",
            external_dag_id="bqetl_experiments_daily",
            external_task_id="wait_for_org_mozilla_ios_firefox_derived__events_stream__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=82800)).isoformat() }}",
        )

        org_mozilla_ios_firefox_derived__events_stream__v1_external.set_upstream(
            org_mozilla_ios_firefox_derived__events_stream__v1
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

    with TaskGroup(
        "org_mozilla_ios_firefoxbeta_derived__events_stream__v1_external",
        parent_group=task_group_firefox_ios,
    ) as org_mozilla_ios_firefoxbeta_derived__events_stream__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_generated_funnels__wait_for_org_mozilla_ios_firefoxbeta_derived__events_stream__v1",
            external_dag_id="bqetl_generated_funnels",
            external_task_id="wait_for_org_mozilla_ios_firefoxbeta_derived__events_stream__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=75600)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_experiments_daily__wait_for_org_mozilla_ios_firefoxbeta_derived__events_stream__v1",
            external_dag_id="bqetl_experiments_daily",
            external_task_id="wait_for_org_mozilla_ios_firefoxbeta_derived__events_stream__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=82800)).isoformat() }}",
        )

        org_mozilla_ios_firefoxbeta_derived__events_stream__v1_external.set_upstream(
            org_mozilla_ios_firefoxbeta_derived__events_stream__v1
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

    with TaskGroup(
        "org_mozilla_ios_focus_derived__baseline_clients_daily__v1_external",
        parent_group=task_group_focus_ios,
    ) as org_mozilla_ios_focus_derived__baseline_clients_daily__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_mobile_kpi_metrics__wait_for_org_mozilla_ios_focus_derived__baseline_clients_daily__v1",
            external_dag_id="bqetl_mobile_kpi_metrics",
            external_task_id="wait_for_org_mozilla_ios_focus_derived__baseline_clients_daily__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=50400)).isoformat() }}",
        )

        org_mozilla_ios_focus_derived__baseline_clients_daily__v1_external.set_upstream(
            org_mozilla_ios_focus_derived__baseline_clients_daily__v1
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

    with TaskGroup(
        "org_mozilla_ios_focus_derived__events_stream__v1_external",
        parent_group=task_group_focus_ios,
    ) as org_mozilla_ios_focus_derived__events_stream__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_experiments_daily__wait_for_org_mozilla_ios_focus_derived__events_stream__v1",
            external_dag_id="bqetl_experiments_daily",
            external_task_id="wait_for_org_mozilla_ios_focus_derived__events_stream__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=82800)).isoformat() }}",
        )

        org_mozilla_ios_focus_derived__events_stream__v1_external.set_upstream(
            org_mozilla_ios_focus_derived__events_stream__v1
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

    with TaskGroup(
        "org_mozilla_ios_klar_derived__baseline_clients_daily__v1_external",
        parent_group=task_group_klar_ios,
    ) as org_mozilla_ios_klar_derived__baseline_clients_daily__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_mobile_kpi_metrics__wait_for_org_mozilla_ios_klar_derived__baseline_clients_daily__v1",
            external_dag_id="bqetl_mobile_kpi_metrics",
            external_task_id="wait_for_org_mozilla_ios_klar_derived__baseline_clients_daily__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=50400)).isoformat() }}",
        )

        org_mozilla_ios_klar_derived__baseline_clients_daily__v1_external.set_upstream(
            org_mozilla_ios_klar_derived__baseline_clients_daily__v1
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

    with TaskGroup(
        "org_mozilla_ios_klar_derived__events_stream__v1_external",
        parent_group=task_group_klar_ios,
    ) as org_mozilla_ios_klar_derived__events_stream__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_experiments_daily__wait_for_org_mozilla_ios_klar_derived__events_stream__v1",
            external_dag_id="bqetl_experiments_daily",
            external_task_id="wait_for_org_mozilla_ios_klar_derived__events_stream__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=82800)).isoformat() }}",
        )

        org_mozilla_ios_klar_derived__events_stream__v1_external.set_upstream(
            org_mozilla_ios_klar_derived__events_stream__v1
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

    with TaskGroup(
        "org_mozilla_klar_derived__baseline_clients_daily__v1_external",
        parent_group=task_group_klar_android,
    ) as org_mozilla_klar_derived__baseline_clients_daily__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_mobile_kpi_metrics__wait_for_org_mozilla_klar_derived__baseline_clients_daily__v1",
            external_dag_id="bqetl_mobile_kpi_metrics",
            external_task_id="wait_for_org_mozilla_klar_derived__baseline_clients_daily__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=50400)).isoformat() }}",
        )

        org_mozilla_klar_derived__baseline_clients_daily__v1_external.set_upstream(
            org_mozilla_klar_derived__baseline_clients_daily__v1
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

    with TaskGroup(
        "org_mozilla_klar_derived__events_stream__v1_external",
        parent_group=task_group_klar_android,
    ) as org_mozilla_klar_derived__events_stream__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_experiments_daily__wait_for_org_mozilla_klar_derived__events_stream__v1",
            external_dag_id="bqetl_experiments_daily",
            external_task_id="wait_for_org_mozilla_klar_derived__events_stream__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=82800)).isoformat() }}",
        )

        org_mozilla_klar_derived__events_stream__v1_external.set_upstream(
            org_mozilla_klar_derived__events_stream__v1
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

    subscription_platform_backend_derived__events_stream__v1 = bigquery_etl_query(
        task_id="subscription_platform_backend_derived__events_stream__v1",
        destination_table="events_stream_v1",
        dataset_id="subscription_platform_backend_derived",
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
        task_group=task_group_subscription_platform_backend,
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

    bigeye__experimenter_cirrus_derived__baseline_clients_daily__v1.set_upstream(
        experimenter_cirrus_derived__baseline_clients_daily__v1
    )

    bigeye__experimenter_cirrus_derived__baseline_clients_first_seen__v1.set_upstream(
        experimenter_cirrus_derived__baseline_clients_first_seen__v1
    )

    bigeye__experimenter_cirrus_derived__baseline_clients_last_seen__v1.set_upstream(
        experimenter_cirrus_derived__baseline_clients_last_seen__v1
    )

    bigeye__experimenter_cirrus_derived__metrics_clients_daily__v1.set_upstream(
        experimenter_cirrus_derived__metrics_clients_daily__v1
    )

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

    checks__fail_experimenter_cirrus_derived__baseline_clients_last_seen__v1.set_upstream(
        experimenter_cirrus_derived__baseline_clients_last_seen__v1
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

    experimenter_cirrus_derived__baseline_clients_daily__v1.set_upstream(
        bigeye__experimenter_cirrus_derived__baseline_clients_first_seen__v1
    )

    experimenter_cirrus_derived__baseline_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    experimenter_cirrus_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    experimenter_cirrus_derived__baseline_clients_first_seen__v1.set_upstream(
        wait_for_telemetry_derived__core_clients_first_seen__v1
    )

    experimenter_cirrus_derived__baseline_clients_last_seen__v1.set_upstream(
        bigeye__experimenter_cirrus_derived__baseline_clients_daily__v1
    )

    experimenter_cirrus_derived__events_stream__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    experimenter_cirrus_derived__metrics_clients_daily__v1.set_upstream(
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

    firefox_fire_tv_derived__metrics_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
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

    firefox_reality_derived__metrics_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    firefox_reality_pc_derived__metrics_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
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

    glam_derived__events_stream__v1.set_upstream(wait_for_copy_deduplicate_all)

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

    mdn_fred_derived__events_stream__v1.set_upstream(wait_for_copy_deduplicate_all)

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

    org_mozilla_tv_firefox_derived__events_stream__v1.set_upstream(
        wait_for_copy_deduplicate_all
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

    subscription_platform_backend_derived__events_stream__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

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
