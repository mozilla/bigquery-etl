# Generated via https://github.com/mozilla/bigquery-etl/blob/main/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskMarker
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
import datetime
from utils.constants import ALLOWED_STATES, FAILED_STATES
from utils.gcp import bigquery_etl_query, gke_command, bigquery_dq_check

docs = """
### bqetl_fxa_events

Built from bigquery-etl repo, [`dags/bqetl_fxa_events.py`](https://github.com/mozilla/bigquery-etl/blob/main/dags/bqetl_fxa_events.py)

#### Description

Copies data from a Firefox Accounts (FxA) project. Those source tables
are populated via Cloud Logging (Stackdriver). We hash various fields
as part of the import.

The DAG also provides daily aggregations on top of the raw log data,
which eventually power high-level reporting about FxA usage.

Tasks here have occasionally failed due to incompatible schema changes
in the tables populated by Cloud Logging.
See https://github.com/mozilla/bigquery-etl/issues/1684 for an example
mitigation.

#### Owner

kik@mozilla.com
"""


default_args = {
    "owner": "kik@mozilla.com",
    "start_date": datetime.datetime(2019, 3, 1, 0, 0),
    "end_date": None,
    "email": ["telemetry-alerts@mozilla.com", "kik@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=600),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
}

tags = ["impact/tier_1", "repo/bigquery-etl"]

with DAG(
    "bqetl_fxa_events",
    default_args=default_args,
    schedule_interval="30 1 * * *",
    doc_md=docs,
    tags=tags,
) as dag:
    docker_fxa_admin_server_v2 = bigquery_etl_query(
        task_id="docker_fxa_admin_server_v2",
        destination_table="docker_fxa_admin_server_sanitized_v2",
        dataset_id="firefox_accounts_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        arguments=["--schema_update_option=ALLOW_FIELD_ADDITION"],
    )

    firefox_accounts_derived__docker_fxa_customs_sanitized__v2 = bigquery_etl_query(
        task_id="firefox_accounts_derived__docker_fxa_customs_sanitized__v2",
        destination_table="docker_fxa_customs_sanitized_v2",
        dataset_id="firefox_accounts_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        arguments=["--schema_update_option=ALLOW_FIELD_ADDITION"],
    )

    firefox_accounts_derived__exact_mau28__v1 = bigquery_etl_query(
        task_id="firefox_accounts_derived__exact_mau28__v1",
        destination_table="exact_mau28_v1",
        dataset_id="firefox_accounts_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    firefox_accounts_derived__fxa_auth_events__v1 = bigquery_etl_query(
        task_id="firefox_accounts_derived__fxa_auth_events__v1",
        destination_table="fxa_auth_events_v1",
        dataset_id="firefox_accounts_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        arguments=["--schema_update_option=ALLOW_FIELD_ADDITION"],
    )

    with TaskGroup(
        "firefox_accounts_derived__fxa_auth_events__v1_external",
    ) as firefox_accounts_derived__fxa_auth_events__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_subplat__wait_for_firefox_accounts_derived__fxa_auth_events__v1",
            external_dag_id="bqetl_subplat",
            external_task_id="wait_for_firefox_accounts_derived__fxa_auth_events__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=85500)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_generated_funnels__wait_for_firefox_accounts_derived__fxa_auth_events__v1",
            external_dag_id="bqetl_generated_funnels",
            external_task_id="wait_for_firefox_accounts_derived__fxa_auth_events__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=73800)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_event_rollup__wait_for_firefox_accounts_derived__fxa_auth_events__v1",
            external_dag_id="bqetl_event_rollup",
            external_task_id="wait_for_firefox_accounts_derived__fxa_auth_events__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=81000)).isoformat() }}",
        )

        firefox_accounts_derived__fxa_auth_events__v1_external.set_upstream(
            firefox_accounts_derived__fxa_auth_events__v1
        )

    firefox_accounts_derived__fxa_content_events__v1 = bigquery_etl_query(
        task_id="firefox_accounts_derived__fxa_content_events__v1",
        destination_table="fxa_content_events_v1",
        dataset_id="firefox_accounts_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        arguments=["--schema_update_option=ALLOW_FIELD_ADDITION"],
    )

    with TaskGroup(
        "firefox_accounts_derived__fxa_content_events__v1_external",
    ) as firefox_accounts_derived__fxa_content_events__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_subplat__wait_for_firefox_accounts_derived__fxa_content_events__v1",
            external_dag_id="bqetl_subplat",
            external_task_id="wait_for_firefox_accounts_derived__fxa_content_events__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=85500)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_generated_funnels__wait_for_firefox_accounts_derived__fxa_content_events__v1",
            external_dag_id="bqetl_generated_funnels",
            external_task_id="wait_for_firefox_accounts_derived__fxa_content_events__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=73800)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_event_rollup__wait_for_firefox_accounts_derived__fxa_content_events__v1",
            external_dag_id="bqetl_event_rollup",
            external_task_id="wait_for_firefox_accounts_derived__fxa_content_events__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=81000)).isoformat() }}",
        )

        firefox_accounts_derived__fxa_content_events__v1_external.set_upstream(
            firefox_accounts_derived__fxa_content_events__v1
        )

    firefox_accounts_derived__fxa_delete_events__v1 = bigquery_etl_query(
        task_id="firefox_accounts_derived__fxa_delete_events__v1",
        destination_table="fxa_delete_events_v1",
        dataset_id="firefox_accounts_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    firefox_accounts_derived__fxa_gcp_stderr_events__v1 = bigquery_etl_query(
        task_id="firefox_accounts_derived__fxa_gcp_stderr_events__v1",
        destination_table="fxa_gcp_stderr_events_v1",
        dataset_id="firefox_accounts_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        start_date=datetime.datetime(2023, 9, 7, 0, 0),
        date_partition_parameter="submission_date",
        depends_on_past=False,
        arguments=["--schema_update_option=ALLOW_FIELD_ADDITION"],
    )

    with TaskGroup(
        "firefox_accounts_derived__fxa_gcp_stderr_events__v1_external",
    ) as firefox_accounts_derived__fxa_gcp_stderr_events__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_subplat__wait_for_firefox_accounts_derived__fxa_gcp_stderr_events__v1",
            external_dag_id="bqetl_subplat",
            external_task_id="wait_for_firefox_accounts_derived__fxa_gcp_stderr_events__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=85500)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_generated_funnels__wait_for_firefox_accounts_derived__fxa_gcp_stderr_events__v1",
            external_dag_id="bqetl_generated_funnels",
            external_task_id="wait_for_firefox_accounts_derived__fxa_gcp_stderr_events__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=73800)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_event_rollup__wait_for_firefox_accounts_derived__fxa_gcp_stderr_events__v1",
            external_dag_id="bqetl_event_rollup",
            external_task_id="wait_for_firefox_accounts_derived__fxa_gcp_stderr_events__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=81000)).isoformat() }}",
        )

        firefox_accounts_derived__fxa_gcp_stderr_events__v1_external.set_upstream(
            firefox_accounts_derived__fxa_gcp_stderr_events__v1
        )

    firefox_accounts_derived__fxa_gcp_stdout_events__v1 = bigquery_etl_query(
        task_id="firefox_accounts_derived__fxa_gcp_stdout_events__v1",
        destination_table="fxa_gcp_stdout_events_v1",
        dataset_id="firefox_accounts_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        start_date=datetime.datetime(2023, 9, 7, 0, 0),
        date_partition_parameter="submission_date",
        depends_on_past=False,
        arguments=["--schema_update_option=ALLOW_FIELD_ADDITION"],
    )

    with TaskGroup(
        "firefox_accounts_derived__fxa_gcp_stdout_events__v1_external",
    ) as firefox_accounts_derived__fxa_gcp_stdout_events__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_subplat__wait_for_firefox_accounts_derived__fxa_gcp_stdout_events__v1",
            external_dag_id="bqetl_subplat",
            external_task_id="wait_for_firefox_accounts_derived__fxa_gcp_stdout_events__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=85500)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_generated_funnels__wait_for_firefox_accounts_derived__fxa_gcp_stdout_events__v1",
            external_dag_id="bqetl_generated_funnels",
            external_task_id="wait_for_firefox_accounts_derived__fxa_gcp_stdout_events__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=73800)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_event_rollup__wait_for_firefox_accounts_derived__fxa_gcp_stdout_events__v1",
            external_dag_id="bqetl_event_rollup",
            external_task_id="wait_for_firefox_accounts_derived__fxa_gcp_stdout_events__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=81000)).isoformat() }}",
        )

        firefox_accounts_derived__fxa_gcp_stdout_events__v1_external.set_upstream(
            firefox_accounts_derived__fxa_gcp_stdout_events__v1
        )

    firefox_accounts_derived__fxa_log_auth_events__v1 = bigquery_etl_query(
        task_id="firefox_accounts_derived__fxa_log_auth_events__v1",
        destination_table="fxa_log_auth_events_v1",
        dataset_id="firefox_accounts_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    firefox_accounts_derived__fxa_log_content_events__v1 = bigquery_etl_query(
        task_id="firefox_accounts_derived__fxa_log_content_events__v1",
        destination_table="fxa_log_content_events_v1",
        dataset_id="firefox_accounts_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    firefox_accounts_derived__fxa_log_device_command_events__v2 = bigquery_etl_query(
        task_id="firefox_accounts_derived__fxa_log_device_command_events__v2",
        destination_table="fxa_log_device_command_events_v2",
        dataset_id="firefox_accounts_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        start_date=datetime.datetime(2023, 9, 7, 0, 0),
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    firefox_accounts_derived__fxa_stdout_events__v1 = bigquery_etl_query(
        task_id="firefox_accounts_derived__fxa_stdout_events__v1",
        destination_table="fxa_stdout_events_v1",
        dataset_id="firefox_accounts_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        arguments=["--schema_update_option=ALLOW_FIELD_ADDITION"],
    )

    with TaskGroup(
        "firefox_accounts_derived__fxa_stdout_events__v1_external",
    ) as firefox_accounts_derived__fxa_stdout_events__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_subplat__wait_for_firefox_accounts_derived__fxa_stdout_events__v1",
            external_dag_id="bqetl_subplat",
            external_task_id="wait_for_firefox_accounts_derived__fxa_stdout_events__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=85500)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_generated_funnels__wait_for_firefox_accounts_derived__fxa_stdout_events__v1",
            external_dag_id="bqetl_generated_funnels",
            external_task_id="wait_for_firefox_accounts_derived__fxa_stdout_events__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=73800)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_event_rollup__wait_for_firefox_accounts_derived__fxa_stdout_events__v1",
            external_dag_id="bqetl_event_rollup",
            external_task_id="wait_for_firefox_accounts_derived__fxa_stdout_events__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=81000)).isoformat() }}",
        )

        firefox_accounts_derived__fxa_stdout_events__v1_external.set_upstream(
            firefox_accounts_derived__fxa_stdout_events__v1
        )

    firefox_accounts_derived__fxa_users_daily__v1 = bigquery_etl_query(
        task_id="firefox_accounts_derived__fxa_users_daily__v1",
        destination_table="fxa_users_daily_v1",
        dataset_id="firefox_accounts_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    firefox_accounts_derived__fxa_users_first_seen__v1 = bigquery_etl_query(
        task_id="firefox_accounts_derived__fxa_users_first_seen__v1",
        destination_table="fxa_users_first_seen_v1",
        dataset_id="firefox_accounts_derived",
        project_id="moz-fx-data-shared-prod",
        owner="frank@mozilla.com",
        email=["frank@mozilla.com", "kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        start_date=datetime.datetime(2021, 7, 9, 0, 0),
        date_partition_parameter=None,
        depends_on_past=True,
        parameters=["submission_date:DATE:{{ds}}"],
        priority_weight=80,
    )

    firefox_accounts_derived__fxa_users_last_seen__v1 = bigquery_etl_query(
        task_id="firefox_accounts_derived__fxa_users_last_seen__v1",
        destination_table="fxa_users_last_seen_v1",
        dataset_id="firefox_accounts_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        start_date=datetime.datetime(2019, 4, 23, 0, 0),
        date_partition_parameter="submission_date",
        depends_on_past=True,
    )

    with TaskGroup(
        "firefox_accounts_derived__fxa_users_last_seen__v1_external",
    ) as firefox_accounts_derived__fxa_users_last_seen__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_gud__wait_for_firefox_accounts_derived__fxa_users_last_seen__v1",
            external_dag_id="bqetl_gud",
            external_task_id="wait_for_firefox_accounts_derived__fxa_users_last_seen__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=81000)).isoformat() }}",
        )

        firefox_accounts_derived__fxa_users_last_seen__v1_external.set_upstream(
            firefox_accounts_derived__fxa_users_last_seen__v1
        )

    firefox_accounts_derived__fxa_users_services_daily__v1 = bigquery_etl_query(
        task_id="firefox_accounts_derived__fxa_users_services_daily__v1",
        destination_table="fxa_users_services_daily_v1",
        dataset_id="firefox_accounts_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    firefox_accounts_derived__fxa_users_services_daily__v2 = bigquery_etl_query(
        task_id="firefox_accounts_derived__fxa_users_services_daily__v2",
        destination_table="fxa_users_services_daily_v2",
        dataset_id="firefox_accounts_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    firefox_accounts_derived__fxa_users_services_devices_daily__v1 = bigquery_etl_query(
        task_id="firefox_accounts_derived__fxa_users_services_devices_daily__v1",
        destination_table="fxa_users_services_devices_daily_v1",
        dataset_id="firefox_accounts_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    firefox_accounts_derived__fxa_users_services_devices_first_seen__v1 = bigquery_etl_query(
        task_id="firefox_accounts_derived__fxa_users_services_devices_first_seen__v1",
        destination_table="fxa_users_services_devices_first_seen_v1",
        dataset_id="firefox_accounts_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=True,
    )

    firefox_accounts_derived__fxa_users_services_devices_last_seen__v1 = bigquery_etl_query(
        task_id="firefox_accounts_derived__fxa_users_services_devices_last_seen__v1",
        destination_table="fxa_users_services_devices_last_seen_v1",
        dataset_id="firefox_accounts_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=True,
    )

    firefox_accounts_derived__fxa_users_services_first_seen__v2 = bigquery_etl_query(
        task_id="firefox_accounts_derived__fxa_users_services_first_seen__v2",
        destination_table="fxa_users_services_first_seen_v2",
        dataset_id="firefox_accounts_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    firefox_accounts_derived__nonprod_fxa_auth_events__v1 = bigquery_etl_query(
        task_id="firefox_accounts_derived__nonprod_fxa_auth_events__v1",
        destination_table="nonprod_fxa_auth_events_v1",
        dataset_id="firefox_accounts_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        arguments=["--schema_update_option=ALLOW_FIELD_ADDITION"],
    )

    firefox_accounts_derived__nonprod_fxa_content_events__v1 = bigquery_etl_query(
        task_id="firefox_accounts_derived__nonprod_fxa_content_events__v1",
        destination_table="nonprod_fxa_content_events_v1",
        dataset_id="firefox_accounts_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        arguments=["--schema_update_option=ALLOW_FIELD_ADDITION"],
    )

    firefox_accounts_derived__nonprod_fxa_gcp_stderr_events__v1 = bigquery_etl_query(
        task_id="firefox_accounts_derived__nonprod_fxa_gcp_stderr_events__v1",
        destination_table="nonprod_fxa_gcp_stderr_events_v1",
        dataset_id="firefox_accounts_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        start_date=datetime.datetime(2023, 5, 26, 0, 0),
        date_partition_parameter="submission_date",
        depends_on_past=False,
        arguments=["--schema_update_option=ALLOW_FIELD_ADDITION"],
    )

    firefox_accounts_derived__nonprod_fxa_gcp_stdout_events__v1 = bigquery_etl_query(
        task_id="firefox_accounts_derived__nonprod_fxa_gcp_stdout_events__v1",
        destination_table="nonprod_fxa_gcp_stdout_events_v1",
        dataset_id="firefox_accounts_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        start_date=datetime.datetime(2023, 5, 26, 0, 0),
        date_partition_parameter="submission_date",
        depends_on_past=False,
        arguments=["--schema_update_option=ALLOW_FIELD_ADDITION"],
    )

    firefox_accounts_derived__nonprod_fxa_stdout_events__v1 = bigquery_etl_query(
        task_id="firefox_accounts_derived__nonprod_fxa_stdout_events__v1",
        destination_table="nonprod_fxa_stdout_events_v1",
        dataset_id="firefox_accounts_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        arguments=["--schema_update_option=ALLOW_FIELD_ADDITION"],
    )

    firefox_accounts_derived__exact_mau28__v1.set_upstream(
        firefox_accounts_derived__fxa_users_last_seen__v1
    )

    firefox_accounts_derived__fxa_users_daily__v1.set_upstream(
        firefox_accounts_derived__fxa_auth_events__v1
    )

    firefox_accounts_derived__fxa_users_daily__v1.set_upstream(
        firefox_accounts_derived__fxa_content_events__v1
    )

    firefox_accounts_derived__fxa_users_daily__v1.set_upstream(
        firefox_accounts_derived__fxa_gcp_stderr_events__v1
    )

    firefox_accounts_derived__fxa_users_daily__v1.set_upstream(
        firefox_accounts_derived__fxa_gcp_stdout_events__v1
    )

    firefox_accounts_derived__fxa_users_daily__v1.set_upstream(
        firefox_accounts_derived__fxa_stdout_events__v1
    )

    firefox_accounts_derived__fxa_users_first_seen__v1.set_upstream(
        firefox_accounts_derived__fxa_auth_events__v1
    )

    firefox_accounts_derived__fxa_users_first_seen__v1.set_upstream(
        firefox_accounts_derived__fxa_content_events__v1
    )

    firefox_accounts_derived__fxa_users_first_seen__v1.set_upstream(
        firefox_accounts_derived__fxa_gcp_stderr_events__v1
    )

    firefox_accounts_derived__fxa_users_first_seen__v1.set_upstream(
        firefox_accounts_derived__fxa_gcp_stdout_events__v1
    )

    firefox_accounts_derived__fxa_users_first_seen__v1.set_upstream(
        firefox_accounts_derived__fxa_stdout_events__v1
    )

    firefox_accounts_derived__fxa_users_last_seen__v1.set_upstream(
        firefox_accounts_derived__fxa_users_daily__v1
    )

    firefox_accounts_derived__fxa_users_services_daily__v1.set_upstream(
        firefox_accounts_derived__fxa_auth_events__v1
    )

    firefox_accounts_derived__fxa_users_services_daily__v1.set_upstream(
        firefox_accounts_derived__fxa_content_events__v1
    )

    firefox_accounts_derived__fxa_users_services_daily__v1.set_upstream(
        firefox_accounts_derived__fxa_gcp_stderr_events__v1
    )

    firefox_accounts_derived__fxa_users_services_daily__v1.set_upstream(
        firefox_accounts_derived__fxa_gcp_stdout_events__v1
    )

    firefox_accounts_derived__fxa_users_services_daily__v1.set_upstream(
        firefox_accounts_derived__fxa_stdout_events__v1
    )

    firefox_accounts_derived__fxa_users_services_daily__v2.set_upstream(
        firefox_accounts_derived__fxa_auth_events__v1
    )

    firefox_accounts_derived__fxa_users_services_daily__v2.set_upstream(
        firefox_accounts_derived__fxa_content_events__v1
    )

    firefox_accounts_derived__fxa_users_services_daily__v2.set_upstream(
        firefox_accounts_derived__fxa_gcp_stderr_events__v1
    )

    firefox_accounts_derived__fxa_users_services_daily__v2.set_upstream(
        firefox_accounts_derived__fxa_gcp_stdout_events__v1
    )

    firefox_accounts_derived__fxa_users_services_daily__v2.set_upstream(
        firefox_accounts_derived__fxa_stdout_events__v1
    )

    firefox_accounts_derived__fxa_users_services_devices_daily__v1.set_upstream(
        firefox_accounts_derived__fxa_auth_events__v1
    )

    firefox_accounts_derived__fxa_users_services_devices_daily__v1.set_upstream(
        firefox_accounts_derived__fxa_content_events__v1
    )

    firefox_accounts_derived__fxa_users_services_devices_daily__v1.set_upstream(
        firefox_accounts_derived__fxa_gcp_stderr_events__v1
    )

    firefox_accounts_derived__fxa_users_services_devices_daily__v1.set_upstream(
        firefox_accounts_derived__fxa_gcp_stdout_events__v1
    )

    firefox_accounts_derived__fxa_users_services_devices_daily__v1.set_upstream(
        firefox_accounts_derived__fxa_stdout_events__v1
    )

    firefox_accounts_derived__fxa_users_services_devices_first_seen__v1.set_upstream(
        firefox_accounts_derived__fxa_users_services_devices_daily__v1
    )

    firefox_accounts_derived__fxa_users_services_devices_last_seen__v1.set_upstream(
        firefox_accounts_derived__fxa_users_services_devices_daily__v1
    )

    firefox_accounts_derived__fxa_users_services_first_seen__v2.set_upstream(
        firefox_accounts_derived__fxa_users_services_daily__v2
    )
