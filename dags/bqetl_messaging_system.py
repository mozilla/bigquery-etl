# Generated via https://github.com/mozilla/bigquery-etl/blob/main/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskMarker
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
import datetime
from utils.constants import ALLOWED_STATES, FAILED_STATES
from utils.gcp import bigquery_etl_query, gke_command, bigquery_dq_check

docs = """
### bqetl_messaging_system

Built from bigquery-etl repo, [`dags/bqetl_messaging_system.py`](https://github.com/mozilla/bigquery-etl/blob/main/dags/bqetl_messaging_system.py)

#### Description

Daily aggregations on top of pings sent for the `messaging_system`
namespace by desktop Firefox.

#### Owner

najiang@mozilla.com
"""


default_args = {
    "owner": "najiang@mozilla.com",
    "start_date": datetime.datetime(2019, 7, 25, 0, 0),
    "end_date": None,
    "email": ["telemetry-alerts@mozilla.com", "najiang@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=300),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
}

tags = ["impact/tier_3", "repo/bigquery-etl"]

with DAG(
    "bqetl_messaging_system",
    default_args=default_args,
    schedule_interval="0 2 * * *",
    doc_md=docs,
    tags=tags,
) as dag:
    firefox_desktop_derived__onboarding__v2 = bigquery_etl_query(
        task_id="firefox_desktop_derived__onboarding__v2",
        destination_table="onboarding_v2",
        dataset_id="firefox_desktop_derived",
        project_id="moz-fx-data-shared-prod",
        owner="gleonard@mozilla.com",
        email=[
            "gleonard@mozilla.com",
            "najiang@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    with TaskGroup(
        "firefox_desktop_derived__onboarding__v2_external"
    ) as firefox_desktop_derived__onboarding__v2_external:
        ExternalTaskMarker(
            task_id="bqetl_review_checker__wait_for_firefox_desktop_derived__onboarding__v2",
            external_dag_id="bqetl_review_checker",
            external_task_id="wait_for_firefox_desktop_derived__onboarding__v2",
            execution_date="{{ (execution_date - macros.timedelta(seconds=7200)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_event_rollup__wait_for_firefox_desktop_derived__onboarding__v2",
            external_dag_id="bqetl_event_rollup",
            external_task_id="wait_for_firefox_desktop_derived__onboarding__v2",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=82800)).isoformat() }}",
        )

        firefox_desktop_derived__onboarding__v2_external.set_upstream(
            firefox_desktop_derived__onboarding__v2
        )

    firefox_desktop_derived__snippets__v2 = bigquery_etl_query(
        task_id="firefox_desktop_derived__snippets__v2",
        destination_table="snippets_v2",
        dataset_id="firefox_desktop_derived",
        project_id="moz-fx-data-shared-prod",
        owner="gleonard@mozilla.com",
        email=[
            "gleonard@mozilla.com",
            "najiang@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    messaging_system_derived__cfr_exact_mau28_by_dimensions__v1 = bigquery_etl_query(
        task_id="messaging_system_derived__cfr_exact_mau28_by_dimensions__v1",
        destination_table="cfr_exact_mau28_by_dimensions_v1",
        dataset_id="messaging_system_derived",
        project_id="moz-fx-data-shared-prod",
        owner="najiang@mozilla.com",
        email=["najiang@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    messaging_system_derived__cfr_users_daily__v1 = bigquery_etl_query(
        task_id="messaging_system_derived__cfr_users_daily__v1",
        destination_table="cfr_users_daily_v1",
        dataset_id="messaging_system_derived",
        project_id="moz-fx-data-shared-prod",
        owner="najiang@mozilla.com",
        email=["najiang@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    messaging_system_derived__cfr_users_last_seen__v1 = bigquery_etl_query(
        task_id="messaging_system_derived__cfr_users_last_seen__v1",
        destination_table="cfr_users_last_seen_v1",
        dataset_id="messaging_system_derived",
        project_id="moz-fx-data-shared-prod",
        owner="najiang@mozilla.com",
        email=["najiang@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=True,
    )

    messaging_system_derived__onboarding_users_daily__v1 = bigquery_etl_query(
        task_id="messaging_system_derived__onboarding_users_daily__v1",
        destination_table="onboarding_users_daily_v1",
        dataset_id="messaging_system_derived",
        project_id="moz-fx-data-shared-prod",
        owner="najiang@mozilla.com",
        email=["najiang@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    messaging_system_derived__onboarding_users_last_seen__v1 = bigquery_etl_query(
        task_id="messaging_system_derived__onboarding_users_last_seen__v1",
        destination_table="onboarding_users_last_seen_v1",
        dataset_id="messaging_system_derived",
        project_id="moz-fx-data-shared-prod",
        owner="najiang@mozilla.com",
        email=["najiang@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=True,
    )

    messaging_system_derived__snippets_users_daily__v1 = bigquery_etl_query(
        task_id="messaging_system_derived__snippets_users_daily__v1",
        destination_table="snippets_users_daily_v1",
        dataset_id="messaging_system_derived",
        project_id="moz-fx-data-shared-prod",
        owner="najiang@mozilla.com",
        email=["najiang@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    messaging_system_derived__snippets_users_last_seen__v1 = bigquery_etl_query(
        task_id="messaging_system_derived__snippets_users_last_seen__v1",
        destination_table="snippets_users_last_seen_v1",
        dataset_id="messaging_system_derived",
        project_id="moz-fx-data-shared-prod",
        owner="najiang@mozilla.com",
        email=["najiang@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=True,
    )

    messaging_system_onboarding_exact_mau28_by_dimensions = bigquery_etl_query(
        task_id="messaging_system_onboarding_exact_mau28_by_dimensions",
        destination_table="onboarding_exact_mau28_by_dimensions_v1",
        dataset_id="messaging_system_derived",
        project_id="moz-fx-data-shared-prod",
        owner="najiang@mozilla.com",
        email=["najiang@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    messaging_system_snippets_exact_mau28_by_dimensions = bigquery_etl_query(
        task_id="messaging_system_snippets_exact_mau28_by_dimensions",
        destination_table="snippets_exact_mau28_by_dimensions_v1",
        dataset_id="messaging_system_derived",
        project_id="moz-fx-data-shared-prod",
        owner="najiang@mozilla.com",
        email=["najiang@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
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

    firefox_desktop_derived__onboarding__v2.set_upstream(wait_for_copy_deduplicate_all)

    firefox_desktop_derived__snippets__v2.set_upstream(wait_for_copy_deduplicate_all)

    messaging_system_derived__cfr_exact_mau28_by_dimensions__v1.set_upstream(
        messaging_system_derived__cfr_users_last_seen__v1
    )

    messaging_system_derived__cfr_users_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    messaging_system_derived__cfr_users_last_seen__v1.set_upstream(
        messaging_system_derived__cfr_users_daily__v1
    )

    messaging_system_derived__onboarding_users_daily__v1.set_upstream(
        firefox_desktop_derived__onboarding__v2
    )

    messaging_system_derived__onboarding_users_last_seen__v1.set_upstream(
        messaging_system_derived__onboarding_users_daily__v1
    )

    messaging_system_derived__snippets_users_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    messaging_system_derived__snippets_users_last_seen__v1.set_upstream(
        messaging_system_derived__snippets_users_daily__v1
    )

    messaging_system_onboarding_exact_mau28_by_dimensions.set_upstream(
        messaging_system_derived__onboarding_users_last_seen__v1
    )

    messaging_system_snippets_exact_mau28_by_dimensions.set_upstream(
        messaging_system_derived__snippets_users_last_seen__v1
    )
