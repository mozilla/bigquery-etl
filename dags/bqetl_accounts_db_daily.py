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
### bqetl_accounts_db_daily

Built from bigquery-etl repo, [`dags/bqetl_accounts_db_daily.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_accounts_db_daily.py)

#### Description

Daily imports from Firefox Accounts (FxA) CloudSQL databases.

More frequent imports run in the `bqetl_accounts_db` DAG.

#### Owner

wclouser@mozilla.com

#### Tags

* impact/tier_3
* repo/bigquery-etl
"""


default_args = {
    "owner": "wclouser@mozilla.com",
    "start_date": datetime.datetime(2026, 6, 4, 0, 0),
    "end_date": None,
    "email": [
        "wclouser@mozilla.com",
        "akomar@mozilla.com",
        "telemetry-alerts@mozilla.com",
    ],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=600),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
    "max_active_tis_per_dag": None,
}

tags = ["impact/tier_3", "repo/bigquery-etl"]

with DAG(
    "bqetl_accounts_db_daily",
    default_args=default_args,
    schedule_interval="30 1 * * *",
    doc_md=docs,
    tags=tags,
    catchup=False,
) as dag:

    wait_for_accounts_db_external__fxa_accounts__v1 = ExternalTaskSensor(
        task_id="wait_for_accounts_db_external__fxa_accounts__v1",
        external_dag_id="bqetl_accounts_db",
        external_task_id="accounts_db_external__fxa_accounts__v1",
        execution_delta=datetime.timedelta(days=-1, seconds=10800),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    accounts_db_derived__accounts_aggregates__v1 = bigquery_etl_query(
        task_id="accounts_db_derived__accounts_aggregates__v1",
        destination_table="accounts_aggregates_v1",
        dataset_id="accounts_db_derived",
        project_id="moz-fx-data-shared-prod",
        owner="akomar@mozilla.com",
        email=[
            "akomar@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wclouser@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    accounts_db_external__fxa_account_customers__v1 = bigquery_etl_query(
        task_id="accounts_db_external__fxa_account_customers__v1",
        destination_table="fxa_account_customers_v1",
        dataset_id="accounts_db_external",
        project_id="moz-fx-data-shared-prod",
        owner="akomar@mozilla.com",
        email=[
            "akomar@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wclouser@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    accounts_db_external__fxa_account_groups__v1 = bigquery_etl_query(
        task_id="accounts_db_external__fxa_account_groups__v1",
        destination_table="fxa_account_groups_v1",
        dataset_id="accounts_db_external",
        project_id="moz-fx-data-shared-prod",
        owner="akomar@mozilla.com",
        email=[
            "akomar@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wclouser@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    accounts_db_external__fxa_account_reset_tokens__v1 = bigquery_etl_query(
        task_id="accounts_db_external__fxa_account_reset_tokens__v1",
        destination_table="fxa_account_reset_tokens_v1",
        dataset_id="accounts_db_external",
        project_id="moz-fx-data-shared-prod",
        owner="akomar@mozilla.com",
        email=[
            "akomar@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wclouser@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    accounts_db_external__fxa_carts__v1 = bigquery_etl_query(
        task_id="accounts_db_external__fxa_carts__v1",
        destination_table="fxa_carts_v1",
        dataset_id="accounts_db_external",
        project_id="moz-fx-data-shared-prod",
        owner="akomar@mozilla.com",
        email=[
            "akomar@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wclouser@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    accounts_db_external__fxa_db_metadata__v1 = bigquery_etl_query(
        task_id="accounts_db_external__fxa_db_metadata__v1",
        destination_table="fxa_db_metadata_v1",
        dataset_id="accounts_db_external",
        project_id="moz-fx-data-shared-prod",
        owner="akomar@mozilla.com",
        email=[
            "akomar@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wclouser@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    accounts_db_external__fxa_deleted_accounts__v1 = bigquery_etl_query(
        task_id="accounts_db_external__fxa_deleted_accounts__v1",
        destination_table="fxa_deleted_accounts_v1",
        dataset_id="accounts_db_external",
        project_id="moz-fx-data-shared-prod",
        owner="wclouser@mozilla.com",
        email=[
            "akomar@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wclouser@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    accounts_db_external__fxa_device_command_identifiers__v1 = bigquery_etl_query(
        task_id="accounts_db_external__fxa_device_command_identifiers__v1",
        destination_table="fxa_device_command_identifiers_v1",
        dataset_id="accounts_db_external",
        project_id="moz-fx-data-shared-prod",
        owner="akomar@mozilla.com",
        email=[
            "akomar@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wclouser@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    accounts_db_external__fxa_device_commands__v1 = bigquery_etl_query(
        task_id="accounts_db_external__fxa_device_commands__v1",
        destination_table="fxa_device_commands_v1",
        dataset_id="accounts_db_external",
        project_id="moz-fx-data-shared-prod",
        owner="akomar@mozilla.com",
        email=[
            "akomar@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wclouser@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    accounts_db_external__fxa_devices__v1 = bigquery_etl_query(
        task_id="accounts_db_external__fxa_devices__v1",
        destination_table="fxa_devices_v1",
        dataset_id="accounts_db_external",
        project_id="moz-fx-data-shared-prod",
        owner="akomar@mozilla.com",
        email=[
            "akomar@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wclouser@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    accounts_db_external__fxa_domain_blocklist__v1 = bigquery_etl_query(
        task_id="accounts_db_external__fxa_domain_blocklist__v1",
        destination_table="fxa_domain_blocklist_v1",
        dataset_id="accounts_db_external",
        project_id="moz-fx-data-shared-prod",
        owner="wclouser@mozilla.com",
        email=[
            "akomar@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wclouser@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    accounts_db_external__fxa_email_blocklist__v1 = bigquery_etl_query(
        task_id="accounts_db_external__fxa_email_blocklist__v1",
        destination_table="fxa_email_blocklist_v1",
        dataset_id="accounts_db_external",
        project_id="moz-fx-data-shared-prod",
        owner="wclouser@mozilla.com",
        email=[
            "akomar@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wclouser@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    accounts_db_external__fxa_email_bounces__v1 = bigquery_etl_query(
        task_id="accounts_db_external__fxa_email_bounces__v1",
        destination_table="fxa_email_bounces_v1",
        dataset_id="accounts_db_external",
        project_id="moz-fx-data-shared-prod",
        owner="akomar@mozilla.com",
        email=[
            "akomar@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wclouser@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    accounts_db_external__fxa_email_types__v1 = bigquery_etl_query(
        task_id="accounts_db_external__fxa_email_types__v1",
        destination_table="fxa_email_types_v1",
        dataset_id="accounts_db_external",
        project_id="moz-fx-data-shared-prod",
        owner="akomar@mozilla.com",
        email=[
            "akomar@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wclouser@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    accounts_db_external__fxa_groups__v1 = bigquery_etl_query(
        task_id="accounts_db_external__fxa_groups__v1",
        destination_table="fxa_groups_v1",
        dataset_id="accounts_db_external",
        project_id="moz-fx-data-shared-prod",
        owner="akomar@mozilla.com",
        email=[
            "akomar@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wclouser@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    accounts_db_external__fxa_key_fetch_tokens__v1 = bigquery_etl_query(
        task_id="accounts_db_external__fxa_key_fetch_tokens__v1",
        destination_table="fxa_key_fetch_tokens_v1",
        dataset_id="accounts_db_external",
        project_id="moz-fx-data-shared-prod",
        owner="akomar@mozilla.com",
        email=[
            "akomar@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wclouser@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    accounts_db_external__fxa_linked_accounts__v1 = bigquery_etl_query(
        task_id="accounts_db_external__fxa_linked_accounts__v1",
        destination_table="fxa_linked_accounts_v1",
        dataset_id="accounts_db_external",
        project_id="moz-fx-data-shared-prod",
        owner="akomar@mozilla.com",
        email=[
            "akomar@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wclouser@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    accounts_db_external__fxa_oauth_clients__v1 = bigquery_etl_query(
        task_id="accounts_db_external__fxa_oauth_clients__v1",
        destination_table="fxa_oauth_clients_v1",
        dataset_id="accounts_db_external",
        project_id="moz-fx-data-shared-prod",
        owner="akomar@mozilla.com",
        email=[
            "akomar@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wclouser@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    accounts_db_external__fxa_oauth_codes__v1 = bigquery_etl_query(
        task_id="accounts_db_external__fxa_oauth_codes__v1",
        destination_table="fxa_oauth_codes_v1",
        dataset_id="accounts_db_external",
        project_id="moz-fx-data-shared-prod",
        owner="akomar@mozilla.com",
        email=[
            "akomar@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wclouser@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    accounts_db_external__fxa_oauth_db_metadata__v1 = bigquery_etl_query(
        task_id="accounts_db_external__fxa_oauth_db_metadata__v1",
        destination_table="fxa_oauth_db_metadata_v1",
        dataset_id="accounts_db_external",
        project_id="moz-fx-data-shared-prod",
        owner="akomar@mozilla.com",
        email=[
            "akomar@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wclouser@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    accounts_db_external__fxa_oauth_refresh_tokens__v1 = bigquery_etl_query(
        task_id="accounts_db_external__fxa_oauth_refresh_tokens__v1",
        destination_table="fxa_oauth_refresh_tokens_v1",
        dataset_id="accounts_db_external",
        project_id="moz-fx-data-shared-prod",
        owner="akomar@mozilla.com",
        email=[
            "akomar@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wclouser@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    accounts_db_external__fxa_oauth_scopes__v1 = bigquery_etl_query(
        task_id="accounts_db_external__fxa_oauth_scopes__v1",
        destination_table="fxa_oauth_scopes_v1",
        dataset_id="accounts_db_external",
        project_id="moz-fx-data-shared-prod",
        owner="akomar@mozilla.com",
        email=[
            "akomar@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wclouser@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    accounts_db_external__fxa_oauth_tokens__v1 = bigquery_etl_query(
        task_id="accounts_db_external__fxa_oauth_tokens__v1",
        destination_table="fxa_oauth_tokens_v1",
        dataset_id="accounts_db_external",
        project_id="moz-fx-data-shared-prod",
        owner="akomar@mozilla.com",
        email=[
            "akomar@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wclouser@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    accounts_db_external__fxa_password_change_tokens__v1 = bigquery_etl_query(
        task_id="accounts_db_external__fxa_password_change_tokens__v1",
        destination_table="fxa_password_change_tokens_v1",
        dataset_id="accounts_db_external",
        project_id="moz-fx-data-shared-prod",
        owner="akomar@mozilla.com",
        email=[
            "akomar@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wclouser@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    accounts_db_external__fxa_password_forgot_tokens__v1 = bigquery_etl_query(
        task_id="accounts_db_external__fxa_password_forgot_tokens__v1",
        destination_table="fxa_password_forgot_tokens_v1",
        dataset_id="accounts_db_external",
        project_id="moz-fx-data-shared-prod",
        owner="akomar@mozilla.com",
        email=[
            "akomar@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wclouser@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    accounts_db_external__fxa_paypal_customers__v1 = bigquery_etl_query(
        task_id="accounts_db_external__fxa_paypal_customers__v1",
        destination_table="fxa_paypal_customers_v1",
        dataset_id="accounts_db_external",
        project_id="moz-fx-data-shared-prod",
        owner="akomar@mozilla.com",
        email=[
            "akomar@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wclouser@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    accounts_db_external__fxa_profile_avatar_providers__v1 = bigquery_etl_query(
        task_id="accounts_db_external__fxa_profile_avatar_providers__v1",
        destination_table="fxa_profile_avatar_providers_v1",
        dataset_id="accounts_db_external",
        project_id="moz-fx-data-shared-prod",
        owner="akomar@mozilla.com",
        email=[
            "akomar@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wclouser@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    accounts_db_external__fxa_profile_avatar_selected__v1 = bigquery_etl_query(
        task_id="accounts_db_external__fxa_profile_avatar_selected__v1",
        destination_table="fxa_profile_avatar_selected_v1",
        dataset_id="accounts_db_external",
        project_id="moz-fx-data-shared-prod",
        owner="akomar@mozilla.com",
        email=[
            "akomar@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wclouser@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    accounts_db_external__fxa_profile_avatars__v1 = bigquery_etl_query(
        task_id="accounts_db_external__fxa_profile_avatars__v1",
        destination_table="fxa_profile_avatars_v1",
        dataset_id="accounts_db_external",
        project_id="moz-fx-data-shared-prod",
        owner="akomar@mozilla.com",
        email=[
            "akomar@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wclouser@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    accounts_db_external__fxa_profile_db_metadata__v1 = bigquery_etl_query(
        task_id="accounts_db_external__fxa_profile_db_metadata__v1",
        destination_table="fxa_profile_db_metadata_v1",
        dataset_id="accounts_db_external",
        project_id="moz-fx-data-shared-prod",
        owner="akomar@mozilla.com",
        email=[
            "akomar@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wclouser@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    accounts_db_external__fxa_profile_profile__v1 = bigquery_etl_query(
        task_id="accounts_db_external__fxa_profile_profile__v1",
        destination_table="fxa_profile_profile_v1",
        dataset_id="accounts_db_external",
        project_id="moz-fx-data-shared-prod",
        owner="akomar@mozilla.com",
        email=[
            "akomar@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wclouser@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    accounts_db_external__fxa_recovery_codes__v1 = bigquery_etl_query(
        task_id="accounts_db_external__fxa_recovery_codes__v1",
        destination_table="fxa_recovery_codes_v1",
        dataset_id="accounts_db_external",
        project_id="moz-fx-data-shared-prod",
        owner="akomar@mozilla.com",
        email=[
            "akomar@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wclouser@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    accounts_db_external__fxa_recovery_keys__v1 = bigquery_etl_query(
        task_id="accounts_db_external__fxa_recovery_keys__v1",
        destination_table="fxa_recovery_keys_v1",
        dataset_id="accounts_db_external",
        project_id="moz-fx-data-shared-prod",
        owner="akomar@mozilla.com",
        email=[
            "akomar@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wclouser@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    accounts_db_external__fxa_recovery_phones__v1 = bigquery_etl_query(
        task_id="accounts_db_external__fxa_recovery_phones__v1",
        destination_table="fxa_recovery_phones_v1",
        dataset_id="accounts_db_external",
        project_id="moz-fx-data-shared-prod",
        owner="wclouser@mozilla.com",
        email=[
            "akomar@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wclouser@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    accounts_db_external__fxa_security_event_names__v1 = bigquery_etl_query(
        task_id="accounts_db_external__fxa_security_event_names__v1",
        destination_table="fxa_security_event_names_v1",
        dataset_id="accounts_db_external",
        project_id="moz-fx-data-shared-prod",
        owner="akomar@mozilla.com",
        email=[
            "akomar@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wclouser@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    accounts_db_external__fxa_sent_emails__v1 = bigquery_etl_query(
        task_id="accounts_db_external__fxa_sent_emails__v1",
        destination_table="fxa_sent_emails_v1",
        dataset_id="accounts_db_external",
        project_id="moz-fx-data-shared-prod",
        owner="akomar@mozilla.com",
        email=[
            "akomar@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wclouser@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    accounts_db_external__fxa_session_tokens__v1 = bigquery_etl_query(
        task_id="accounts_db_external__fxa_session_tokens__v1",
        destination_table="fxa_session_tokens_v1",
        dataset_id="accounts_db_external",
        project_id="moz-fx-data-shared-prod",
        owner="akomar@mozilla.com",
        email=[
            "akomar@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wclouser@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    accounts_db_external__fxa_signin_codes__v1 = bigquery_etl_query(
        task_id="accounts_db_external__fxa_signin_codes__v1",
        destination_table="fxa_signin_codes_v1",
        dataset_id="accounts_db_external",
        project_id="moz-fx-data-shared-prod",
        owner="akomar@mozilla.com",
        email=[
            "akomar@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wclouser@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    accounts_db_external__fxa_totp__v1 = bigquery_etl_query(
        task_id="accounts_db_external__fxa_totp__v1",
        destination_table="fxa_totp_v1",
        dataset_id="accounts_db_external",
        project_id="moz-fx-data-shared-prod",
        owner="akomar@mozilla.com",
        email=[
            "akomar@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wclouser@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    accounts_db_external__fxa_unblock_codes__v1 = bigquery_etl_query(
        task_id="accounts_db_external__fxa_unblock_codes__v1",
        destination_table="fxa_unblock_codes_v1",
        dataset_id="accounts_db_external",
        project_id="moz-fx-data-shared-prod",
        owner="akomar@mozilla.com",
        email=[
            "akomar@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wclouser@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    accounts_db_external__fxa_unverified_tokens__v1 = bigquery_etl_query(
        task_id="accounts_db_external__fxa_unverified_tokens__v1",
        destination_table="fxa_unverified_tokens_v1",
        dataset_id="accounts_db_external",
        project_id="moz-fx-data-shared-prod",
        owner="akomar@mozilla.com",
        email=[
            "akomar@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wclouser@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    accounts_db_external__fxa_verification_reminders__v1 = bigquery_etl_query(
        task_id="accounts_db_external__fxa_verification_reminders__v1",
        destination_table="fxa_verification_reminders_v1",
        dataset_id="accounts_db_external",
        project_id="moz-fx-data-shared-prod",
        owner="akomar@mozilla.com",
        email=[
            "akomar@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wclouser@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    accounts_db_nonprod_external__fxa_account_customers__v1 = bigquery_etl_query(
        task_id="accounts_db_nonprod_external__fxa_account_customers__v1",
        destination_table="fxa_account_customers_v1",
        dataset_id="accounts_db_nonprod_external",
        project_id="moz-fx-data-shared-prod",
        owner="akomar@mozilla.com",
        email=[
            "akomar@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wclouser@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    accounts_db_nonprod_external__fxa_account_groups__v1 = bigquery_etl_query(
        task_id="accounts_db_nonprod_external__fxa_account_groups__v1",
        destination_table="fxa_account_groups_v1",
        dataset_id="accounts_db_nonprod_external",
        project_id="moz-fx-data-shared-prod",
        owner="akomar@mozilla.com",
        email=[
            "akomar@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wclouser@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    accounts_db_nonprod_external__fxa_account_reset_tokens__v1 = bigquery_etl_query(
        task_id="accounts_db_nonprod_external__fxa_account_reset_tokens__v1",
        destination_table="fxa_account_reset_tokens_v1",
        dataset_id="accounts_db_nonprod_external",
        project_id="moz-fx-data-shared-prod",
        owner="akomar@mozilla.com",
        email=[
            "akomar@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wclouser@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    accounts_db_nonprod_external__fxa_accounts__v1 = bigquery_etl_query(
        task_id="accounts_db_nonprod_external__fxa_accounts__v1",
        destination_table="fxa_accounts_v1",
        dataset_id="accounts_db_nonprod_external",
        project_id="moz-fx-data-shared-prod",
        owner="akomar@mozilla.com",
        email=[
            "akomar@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wclouser@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    accounts_db_nonprod_external__fxa_carts__v1 = bigquery_etl_query(
        task_id="accounts_db_nonprod_external__fxa_carts__v1",
        destination_table="fxa_carts_v1",
        dataset_id="accounts_db_nonprod_external",
        project_id="moz-fx-data-shared-prod",
        owner="akomar@mozilla.com",
        email=[
            "akomar@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wclouser@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    accounts_db_nonprod_external__fxa_db_metadata__v1 = bigquery_etl_query(
        task_id="accounts_db_nonprod_external__fxa_db_metadata__v1",
        destination_table="fxa_db_metadata_v1",
        dataset_id="accounts_db_nonprod_external",
        project_id="moz-fx-data-shared-prod",
        owner="akomar@mozilla.com",
        email=[
            "akomar@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wclouser@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    accounts_db_nonprod_external__fxa_deleted_accounts__v1 = bigquery_etl_query(
        task_id="accounts_db_nonprod_external__fxa_deleted_accounts__v1",
        destination_table="fxa_deleted_accounts_v1",
        dataset_id="accounts_db_nonprod_external",
        project_id="moz-fx-data-shared-prod",
        owner="wclouser@mozilla.com",
        email=[
            "akomar@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wclouser@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    accounts_db_nonprod_external__fxa_device_command_identifiers__v1 = (
        bigquery_etl_query(
            task_id="accounts_db_nonprod_external__fxa_device_command_identifiers__v1",
            destination_table="fxa_device_command_identifiers_v1",
            dataset_id="accounts_db_nonprod_external",
            project_id="moz-fx-data-shared-prod",
            owner="akomar@mozilla.com",
            email=[
                "akomar@mozilla.com",
                "telemetry-alerts@mozilla.com",
                "wclouser@mozilla.com",
            ],
            date_partition_parameter=None,
            depends_on_past=False,
            task_concurrency=1,
        )
    )

    accounts_db_nonprod_external__fxa_device_commands__v1 = bigquery_etl_query(
        task_id="accounts_db_nonprod_external__fxa_device_commands__v1",
        destination_table="fxa_device_commands_v1",
        dataset_id="accounts_db_nonprod_external",
        project_id="moz-fx-data-shared-prod",
        owner="akomar@mozilla.com",
        email=[
            "akomar@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wclouser@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    accounts_db_nonprod_external__fxa_devices__v1 = bigquery_etl_query(
        task_id="accounts_db_nonprod_external__fxa_devices__v1",
        destination_table="fxa_devices_v1",
        dataset_id="accounts_db_nonprod_external",
        project_id="moz-fx-data-shared-prod",
        owner="akomar@mozilla.com",
        email=[
            "akomar@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wclouser@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    accounts_db_nonprod_external__fxa_domain_blocklist__v1 = bigquery_etl_query(
        task_id="accounts_db_nonprod_external__fxa_domain_blocklist__v1",
        destination_table="fxa_domain_blocklist_v1",
        dataset_id="accounts_db_nonprod_external",
        project_id="moz-fx-data-shared-prod",
        owner="wclouser@mozilla.com",
        email=[
            "akomar@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wclouser@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    accounts_db_nonprod_external__fxa_email_blocklist__v1 = bigquery_etl_query(
        task_id="accounts_db_nonprod_external__fxa_email_blocklist__v1",
        destination_table="fxa_email_blocklist_v1",
        dataset_id="accounts_db_nonprod_external",
        project_id="moz-fx-data-shared-prod",
        owner="wclouser@mozilla.com",
        email=[
            "akomar@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wclouser@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    accounts_db_nonprod_external__fxa_email_bounces__v1 = bigquery_etl_query(
        task_id="accounts_db_nonprod_external__fxa_email_bounces__v1",
        destination_table="fxa_email_bounces_v1",
        dataset_id="accounts_db_nonprod_external",
        project_id="moz-fx-data-shared-prod",
        owner="akomar@mozilla.com",
        email=[
            "akomar@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wclouser@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    accounts_db_nonprod_external__fxa_email_types__v1 = bigquery_etl_query(
        task_id="accounts_db_nonprod_external__fxa_email_types__v1",
        destination_table="fxa_email_types_v1",
        dataset_id="accounts_db_nonprod_external",
        project_id="moz-fx-data-shared-prod",
        owner="akomar@mozilla.com",
        email=[
            "akomar@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wclouser@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    accounts_db_nonprod_external__fxa_emails__v1 = bigquery_etl_query(
        task_id="accounts_db_nonprod_external__fxa_emails__v1",
        destination_table="fxa_emails_v1",
        dataset_id="accounts_db_nonprod_external",
        project_id="moz-fx-data-shared-prod",
        owner="akomar@mozilla.com",
        email=[
            "akomar@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wclouser@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    accounts_db_nonprod_external__fxa_groups__v1 = bigquery_etl_query(
        task_id="accounts_db_nonprod_external__fxa_groups__v1",
        destination_table="fxa_groups_v1",
        dataset_id="accounts_db_nonprod_external",
        project_id="moz-fx-data-shared-prod",
        owner="akomar@mozilla.com",
        email=[
            "akomar@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wclouser@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    accounts_db_nonprod_external__fxa_key_fetch_tokens__v1 = bigquery_etl_query(
        task_id="accounts_db_nonprod_external__fxa_key_fetch_tokens__v1",
        destination_table="fxa_key_fetch_tokens_v1",
        dataset_id="accounts_db_nonprod_external",
        project_id="moz-fx-data-shared-prod",
        owner="akomar@mozilla.com",
        email=[
            "akomar@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wclouser@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    accounts_db_nonprod_external__fxa_linked_accounts__v1 = bigquery_etl_query(
        task_id="accounts_db_nonprod_external__fxa_linked_accounts__v1",
        destination_table="fxa_linked_accounts_v1",
        dataset_id="accounts_db_nonprod_external",
        project_id="moz-fx-data-shared-prod",
        owner="akomar@mozilla.com",
        email=[
            "akomar@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wclouser@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    accounts_db_nonprod_external__fxa_oauth_account_authorizations__v1 = bigquery_etl_query(
        task_id="accounts_db_nonprod_external__fxa_oauth_account_authorizations__v1",
        destination_table="fxa_oauth_account_authorizations_v1",
        dataset_id="accounts_db_nonprod_external",
        project_id="moz-fx-data-shared-prod",
        owner="wclouser@mozilla.com",
        email=[
            "akomar@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wclouser@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    accounts_db_nonprod_external__fxa_oauth_clients__v1 = bigquery_etl_query(
        task_id="accounts_db_nonprod_external__fxa_oauth_clients__v1",
        destination_table="fxa_oauth_clients_v1",
        dataset_id="accounts_db_nonprod_external",
        project_id="moz-fx-data-shared-prod",
        owner="akomar@mozilla.com",
        email=[
            "akomar@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wclouser@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    accounts_db_nonprod_external__fxa_oauth_codes__v1 = bigquery_etl_query(
        task_id="accounts_db_nonprod_external__fxa_oauth_codes__v1",
        destination_table="fxa_oauth_codes_v1",
        dataset_id="accounts_db_nonprod_external",
        project_id="moz-fx-data-shared-prod",
        owner="akomar@mozilla.com",
        email=[
            "akomar@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wclouser@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    accounts_db_nonprod_external__fxa_oauth_db_metadata__v1 = bigquery_etl_query(
        task_id="accounts_db_nonprod_external__fxa_oauth_db_metadata__v1",
        destination_table="fxa_oauth_db_metadata_v1",
        dataset_id="accounts_db_nonprod_external",
        project_id="moz-fx-data-shared-prod",
        owner="akomar@mozilla.com",
        email=[
            "akomar@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wclouser@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    accounts_db_nonprod_external__fxa_oauth_refresh_tokens__v1 = bigquery_etl_query(
        task_id="accounts_db_nonprod_external__fxa_oauth_refresh_tokens__v1",
        destination_table="fxa_oauth_refresh_tokens_v1",
        dataset_id="accounts_db_nonprod_external",
        project_id="moz-fx-data-shared-prod",
        owner="akomar@mozilla.com",
        email=[
            "akomar@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wclouser@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    accounts_db_nonprod_external__fxa_oauth_scopes__v1 = bigquery_etl_query(
        task_id="accounts_db_nonprod_external__fxa_oauth_scopes__v1",
        destination_table="fxa_oauth_scopes_v1",
        dataset_id="accounts_db_nonprod_external",
        project_id="moz-fx-data-shared-prod",
        owner="akomar@mozilla.com",
        email=[
            "akomar@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wclouser@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    accounts_db_nonprod_external__fxa_oauth_tokens__v1 = bigquery_etl_query(
        task_id="accounts_db_nonprod_external__fxa_oauth_tokens__v1",
        destination_table="fxa_oauth_tokens_v1",
        dataset_id="accounts_db_nonprod_external",
        project_id="moz-fx-data-shared-prod",
        owner="akomar@mozilla.com",
        email=[
            "akomar@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wclouser@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    accounts_db_nonprod_external__fxa_password_change_tokens__v1 = bigquery_etl_query(
        task_id="accounts_db_nonprod_external__fxa_password_change_tokens__v1",
        destination_table="fxa_password_change_tokens_v1",
        dataset_id="accounts_db_nonprod_external",
        project_id="moz-fx-data-shared-prod",
        owner="akomar@mozilla.com",
        email=[
            "akomar@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wclouser@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    accounts_db_nonprod_external__fxa_password_forgot_tokens__v1 = bigquery_etl_query(
        task_id="accounts_db_nonprod_external__fxa_password_forgot_tokens__v1",
        destination_table="fxa_password_forgot_tokens_v1",
        dataset_id="accounts_db_nonprod_external",
        project_id="moz-fx-data-shared-prod",
        owner="akomar@mozilla.com",
        email=[
            "akomar@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wclouser@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    accounts_db_nonprod_external__fxa_paypal_customers__v1 = bigquery_etl_query(
        task_id="accounts_db_nonprod_external__fxa_paypal_customers__v1",
        destination_table="fxa_paypal_customers_v1",
        dataset_id="accounts_db_nonprod_external",
        project_id="moz-fx-data-shared-prod",
        owner="akomar@mozilla.com",
        email=[
            "akomar@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wclouser@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    accounts_db_nonprod_external__fxa_profile_avatar_providers__v1 = bigquery_etl_query(
        task_id="accounts_db_nonprod_external__fxa_profile_avatar_providers__v1",
        destination_table="fxa_profile_avatar_providers_v1",
        dataset_id="accounts_db_nonprod_external",
        project_id="moz-fx-data-shared-prod",
        owner="akomar@mozilla.com",
        email=[
            "akomar@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wclouser@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    accounts_db_nonprod_external__fxa_profile_avatar_selected__v1 = bigquery_etl_query(
        task_id="accounts_db_nonprod_external__fxa_profile_avatar_selected__v1",
        destination_table="fxa_profile_avatar_selected_v1",
        dataset_id="accounts_db_nonprod_external",
        project_id="moz-fx-data-shared-prod",
        owner="akomar@mozilla.com",
        email=[
            "akomar@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wclouser@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    accounts_db_nonprod_external__fxa_profile_avatars__v1 = bigquery_etl_query(
        task_id="accounts_db_nonprod_external__fxa_profile_avatars__v1",
        destination_table="fxa_profile_avatars_v1",
        dataset_id="accounts_db_nonprod_external",
        project_id="moz-fx-data-shared-prod",
        owner="akomar@mozilla.com",
        email=[
            "akomar@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wclouser@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    accounts_db_nonprod_external__fxa_profile_db_metadata__v1 = bigquery_etl_query(
        task_id="accounts_db_nonprod_external__fxa_profile_db_metadata__v1",
        destination_table="fxa_profile_db_metadata_v1",
        dataset_id="accounts_db_nonprod_external",
        project_id="moz-fx-data-shared-prod",
        owner="akomar@mozilla.com",
        email=[
            "akomar@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wclouser@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    accounts_db_nonprod_external__fxa_profile_profile__v1 = bigquery_etl_query(
        task_id="accounts_db_nonprod_external__fxa_profile_profile__v1",
        destination_table="fxa_profile_profile_v1",
        dataset_id="accounts_db_nonprod_external",
        project_id="moz-fx-data-shared-prod",
        owner="akomar@mozilla.com",
        email=[
            "akomar@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wclouser@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    accounts_db_nonprod_external__fxa_recovery_codes__v1 = bigquery_etl_query(
        task_id="accounts_db_nonprod_external__fxa_recovery_codes__v1",
        destination_table="fxa_recovery_codes_v1",
        dataset_id="accounts_db_nonprod_external",
        project_id="moz-fx-data-shared-prod",
        owner="akomar@mozilla.com",
        email=[
            "akomar@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wclouser@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    accounts_db_nonprod_external__fxa_recovery_keys__v1 = bigquery_etl_query(
        task_id="accounts_db_nonprod_external__fxa_recovery_keys__v1",
        destination_table="fxa_recovery_keys_v1",
        dataset_id="accounts_db_nonprod_external",
        project_id="moz-fx-data-shared-prod",
        owner="akomar@mozilla.com",
        email=[
            "akomar@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wclouser@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    accounts_db_nonprod_external__fxa_recovery_phones__v1 = bigquery_etl_query(
        task_id="accounts_db_nonprod_external__fxa_recovery_phones__v1",
        destination_table="fxa_recovery_phones_v1",
        dataset_id="accounts_db_nonprod_external",
        project_id="moz-fx-data-shared-prod",
        owner="wclouser@mozilla.com",
        email=[
            "akomar@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wclouser@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    accounts_db_nonprod_external__fxa_security_event_names__v1 = bigquery_etl_query(
        task_id="accounts_db_nonprod_external__fxa_security_event_names__v1",
        destination_table="fxa_security_event_names_v1",
        dataset_id="accounts_db_nonprod_external",
        project_id="moz-fx-data-shared-prod",
        owner="akomar@mozilla.com",
        email=[
            "akomar@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wclouser@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    accounts_db_nonprod_external__fxa_security_events__v1 = bigquery_etl_query(
        task_id="accounts_db_nonprod_external__fxa_security_events__v1",
        destination_table="fxa_security_events_v1",
        dataset_id="accounts_db_nonprod_external",
        project_id="moz-fx-data-shared-prod",
        owner="akomar@mozilla.com",
        email=[
            "akomar@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wclouser@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    accounts_db_nonprod_external__fxa_sent_emails__v1 = bigquery_etl_query(
        task_id="accounts_db_nonprod_external__fxa_sent_emails__v1",
        destination_table="fxa_sent_emails_v1",
        dataset_id="accounts_db_nonprod_external",
        project_id="moz-fx-data-shared-prod",
        owner="akomar@mozilla.com",
        email=[
            "akomar@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wclouser@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    accounts_db_nonprod_external__fxa_session_tokens__v1 = bigquery_etl_query(
        task_id="accounts_db_nonprod_external__fxa_session_tokens__v1",
        destination_table="fxa_session_tokens_v1",
        dataset_id="accounts_db_nonprod_external",
        project_id="moz-fx-data-shared-prod",
        owner="akomar@mozilla.com",
        email=[
            "akomar@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wclouser@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    accounts_db_nonprod_external__fxa_signin_codes__v1 = bigquery_etl_query(
        task_id="accounts_db_nonprod_external__fxa_signin_codes__v1",
        destination_table="fxa_signin_codes_v1",
        dataset_id="accounts_db_nonprod_external",
        project_id="moz-fx-data-shared-prod",
        owner="akomar@mozilla.com",
        email=[
            "akomar@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wclouser@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    accounts_db_nonprod_external__fxa_totp__v1 = bigquery_etl_query(
        task_id="accounts_db_nonprod_external__fxa_totp__v1",
        destination_table="fxa_totp_v1",
        dataset_id="accounts_db_nonprod_external",
        project_id="moz-fx-data-shared-prod",
        owner="akomar@mozilla.com",
        email=[
            "akomar@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wclouser@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    accounts_db_nonprod_external__fxa_unblock_codes__v1 = bigquery_etl_query(
        task_id="accounts_db_nonprod_external__fxa_unblock_codes__v1",
        destination_table="fxa_unblock_codes_v1",
        dataset_id="accounts_db_nonprod_external",
        project_id="moz-fx-data-shared-prod",
        owner="akomar@mozilla.com",
        email=[
            "akomar@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wclouser@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    accounts_db_nonprod_external__fxa_unverified_tokens__v1 = bigquery_etl_query(
        task_id="accounts_db_nonprod_external__fxa_unverified_tokens__v1",
        destination_table="fxa_unverified_tokens_v1",
        dataset_id="accounts_db_nonprod_external",
        project_id="moz-fx-data-shared-prod",
        owner="akomar@mozilla.com",
        email=[
            "akomar@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wclouser@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    accounts_db_nonprod_external__fxa_verification_reminders__v1 = bigquery_etl_query(
        task_id="accounts_db_nonprod_external__fxa_verification_reminders__v1",
        destination_table="fxa_verification_reminders_v1",
        dataset_id="accounts_db_nonprod_external",
        project_id="moz-fx-data-shared-prod",
        owner="akomar@mozilla.com",
        email=[
            "akomar@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wclouser@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    accounts_db_derived__accounts_aggregates__v1.set_upstream(
        wait_for_accounts_db_external__fxa_accounts__v1
    )

    accounts_db_derived__accounts_aggregates__v1.set_upstream(
        accounts_db_external__fxa_recovery_keys__v1
    )

    accounts_db_derived__accounts_aggregates__v1.set_upstream(
        accounts_db_external__fxa_totp__v1
    )
