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
### bqetl_newtab

Built from bigquery-etl repo, [`dags/bqetl_newtab.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_newtab.py)

#### Description

Schedules newtab related queries.
#### Owner

mbowerman@mozilla.com

#### Tags

* impact/tier_1
* repo/bigquery-etl
"""


default_args = {
    "owner": "mbowerman@mozilla.com",
    "start_date": datetime.datetime(2022, 7, 1, 0, 0),
    "end_date": None,
    "email": ["telemetry-alerts@mozilla.com", "mbowerman@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
    "max_active_tis_per_dag": None,
}

tags = ["impact/tier_1", "repo/bigquery-etl"]

with DAG(
    "bqetl_newtab",
    default_args=default_args,
    schedule_interval="@daily",
    doc_md=docs,
    tags=tags,
    catchup=False,
) as dag:

    wait_for_copy_deduplicate_all = ExternalTaskSensor(
        task_id="wait_for_copy_deduplicate_all",
        external_dag_id="copy_deduplicate",
        external_task_id="copy_deduplicate_all",
        execution_delta=datetime.timedelta(days=-1, seconds=82800),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_snowflake_migration_derived__corpus_items_updated__v1 = ExternalTaskSensor(
        task_id="wait_for_snowflake_migration_derived__corpus_items_updated__v1",
        external_dag_id="bqetl_content_ml_hourly",
        external_task_id="snowflake_migration_derived__corpus_items_updated__v1",
        execution_delta=datetime.timedelta(days=-1, seconds=84600),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_telemetry_derived__newtab_clients_daily__v1 = ExternalTaskSensor(
        task_id="wait_for_telemetry_derived__newtab_clients_daily__v1",
        external_dag_id="bqetl_newtab_late_morning",
        external_task_id="telemetry_derived__newtab_clients_daily__v1",
        execution_delta=datetime.timedelta(days=-1, seconds=50400),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_checks__fail_telemetry_derived__clients_last_seen__v2 = ExternalTaskSensor(
        task_id="wait_for_checks__fail_telemetry_derived__clients_last_seen__v2",
        external_dag_id="bqetl_main_summary",
        external_task_id="checks__fail_telemetry_derived__clients_last_seen__v2",
        execution_delta=datetime.timedelta(days=-1, seconds=79200),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    firefox_desktop_derived__newtab_clients_daily__v2 = bigquery_etl_query(
        task_id="firefox_desktop_derived__newtab_clients_daily__v2",
        destination_table="newtab_clients_daily_v2",
        dataset_id="firefox_desktop_derived",
        project_id="moz-fx-data-shared-prod",
        owner="gkatre@mozilla.com",
        email=[
            "gkatre@mozilla.com",
            "mbowerman@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    with TaskGroup(
        "firefox_desktop_derived__newtab_clients_daily__v2_external",
    ) as firefox_desktop_derived__newtab_clients_daily__v2_external:
        ExternalTaskMarker(
            task_id="private_bqetl_ads_monthly__wait_for_firefox_desktop_derived__newtab_clients_daily__v2",
            external_dag_id="private_bqetl_ads_monthly",
            external_task_id="wait_for_firefox_desktop_derived__newtab_clients_daily__v2",
            execution_date="{{ (execution_date - macros.timedelta(days=-2, seconds=68400)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="private_bqetl_ads_quarterly__wait_for_firefox_desktop_derived__newtab_clients_daily__v2",
            external_dag_id="private_bqetl_ads_quarterly",
            external_task_id="wait_for_firefox_desktop_derived__newtab_clients_daily__v2",
        )

        firefox_desktop_derived__newtab_clients_daily__v2_external.set_upstream(
            firefox_desktop_derived__newtab_clients_daily__v2
        )

    firefox_desktop_derived__newtab_clients_daily_aggregates__v2 = bigquery_etl_query(
        task_id="firefox_desktop_derived__newtab_clients_daily_aggregates__v2",
        destination_table="newtab_clients_daily_aggregates_v2",
        dataset_id="firefox_desktop_derived",
        project_id="moz-fx-data-shared-prod",
        owner="gkatre@mozilla.com",
        email=[
            "gkatre@mozilla.com",
            "mbowerman@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    firefox_desktop_derived__newtab_component_content__v1 = bigquery_etl_query(
        task_id="firefox_desktop_derived__newtab_component_content__v1",
        destination_table="newtab_component_content_v1",
        dataset_id="firefox_desktop_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jsnyder@mozilla.com",
        email=[
            "jsnyder@mozilla.com",
            "mbowerman@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    with TaskGroup(
        "firefox_desktop_derived__newtab_component_content__v1_external",
    ) as firefox_desktop_derived__newtab_component_content__v1_external:
        ExternalTaskMarker(
            task_id="private_bqetl_ads__wait_for_firefox_desktop_derived__newtab_component_content__v1",
            external_dag_id="private_bqetl_ads",
            external_task_id="wait_for_firefox_desktop_derived__newtab_component_content__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=72000)).isoformat() }}",
        )

        firefox_desktop_derived__newtab_component_content__v1_external.set_upstream(
            firefox_desktop_derived__newtab_component_content__v1
        )

    firefox_desktop_derived__newtab_items_daily__v1 = bigquery_etl_query(
        task_id="firefox_desktop_derived__newtab_items_daily__v1",
        destination_table="newtab_items_daily_v1",
        dataset_id="firefox_desktop_derived",
        project_id="moz-fx-data-shared-prod",
        owner="gkatre@mozilla.com",
        email=[
            "gkatre@mozilla.com",
            "mbowerman@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    firefox_desktop_derived__newtab_visits_daily__v2 = bigquery_etl_query(
        task_id="firefox_desktop_derived__newtab_visits_daily__v2",
        destination_table="newtab_visits_daily_v2",
        dataset_id="firefox_desktop_derived",
        project_id="moz-fx-data-shared-prod",
        owner="gkatre@mozilla.com",
        email=[
            "gkatre@mozilla.com",
            "mbowerman@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    with TaskGroup(
        "firefox_desktop_derived__newtab_visits_daily__v2_external",
    ) as firefox_desktop_derived__newtab_visits_daily__v2_external:
        ExternalTaskMarker(
            task_id="private_bqetl_ads__wait_for_firefox_desktop_derived__newtab_visits_daily__v2",
            external_dag_id="private_bqetl_ads",
            external_task_id="wait_for_firefox_desktop_derived__newtab_visits_daily__v2",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=72000)).isoformat() }}",
        )

        firefox_desktop_derived__newtab_visits_daily__v2_external.set_upstream(
            firefox_desktop_derived__newtab_visits_daily__v2
        )

    firefox_desktop_derived__report_content__v1 = bigquery_etl_query(
        task_id="firefox_desktop_derived__report_content__v1",
        destination_table="report_content_v1",
        dataset_id="firefox_desktop_derived",
        project_id="moz-fx-data-shared-prod",
        owner="isegall@mozilla.com",
        email=[
            "isegall@mozilla.com",
            "mbowerman@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    telemetry_derived__newtab_clients_daily_aggregates__v1 = bigquery_etl_query(
        task_id="telemetry_derived__newtab_clients_daily_aggregates__v1",
        destination_table="newtab_clients_daily_aggregates_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="cbeck@mozilla.com",
        email=[
            "cbeck@mozilla.com",
            "mbowerman@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    telemetry_derived__newtab_conditional_daily_aggregates__v1 = bigquery_etl_query(
        task_id="telemetry_derived__newtab_conditional_daily_aggregates__v1",
        destination_table="newtab_conditional_daily_aggregates_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="gkatre@mozilla.com",
        email=[
            "gkatre@mozilla.com",
            "mbowerman@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    telemetry_derived__newtab_daily_interactions_aggregates__v1 = bigquery_etl_query(
        task_id="telemetry_derived__newtab_daily_interactions_aggregates__v1",
        destination_table="newtab_daily_interactions_aggregates_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="gkatre@mozilla.com",
        email=[
            "gkatre@mozilla.com",
            "mbowerman@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    telemetry_derived__newtab_interactions__v1 = bigquery_etl_query(
        task_id="telemetry_derived__newtab_interactions__v1",
        destination_table="newtab_interactions_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="cbeck@mozilla.com",
        email=[
            "cbeck@mozilla.com",
            "mbowerman@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    telemetry_derived__newtab_visits__v1 = bigquery_etl_query(
        task_id="telemetry_derived__newtab_visits__v1",
        destination_table="newtab_visits_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="mbowerman@mozilla.com",
        email=[
            "cbeck@mozilla.com",
            "mbowerman@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    with TaskGroup(
        "telemetry_derived__newtab_visits__v1_external",
    ) as telemetry_derived__newtab_visits__v1_external:
        ExternalTaskMarker(
            task_id="private_bqetl_ads__wait_for_telemetry_derived__newtab_visits__v1",
            external_dag_id="private_bqetl_ads",
            external_task_id="wait_for_telemetry_derived__newtab_visits__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-2, seconds=72000)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_newtab_late_morning__wait_for_telemetry_derived__newtab_visits__v1",
            external_dag_id="bqetl_newtab_late_morning",
            external_task_id="wait_for_telemetry_derived__newtab_visits__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=50400)).isoformat() }}",
        )

        telemetry_derived__newtab_visits__v1_external.set_upstream(
            telemetry_derived__newtab_visits__v1
        )

    firefox_desktop_derived__newtab_clients_daily__v2.set_upstream(
        firefox_desktop_derived__newtab_visits_daily__v2
    )

    firefox_desktop_derived__newtab_clients_daily_aggregates__v2.set_upstream(
        firefox_desktop_derived__newtab_clients_daily__v2
    )

    firefox_desktop_derived__newtab_component_content__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    firefox_desktop_derived__newtab_items_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    firefox_desktop_derived__newtab_items_daily__v1.set_upstream(
        wait_for_snowflake_migration_derived__corpus_items_updated__v1
    )

    firefox_desktop_derived__newtab_visits_daily__v2.set_upstream(
        wait_for_copy_deduplicate_all
    )

    firefox_desktop_derived__report_content__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    telemetry_derived__newtab_clients_daily_aggregates__v1.set_upstream(
        wait_for_telemetry_derived__newtab_clients_daily__v1
    )

    telemetry_derived__newtab_conditional_daily_aggregates__v1.set_upstream(
        telemetry_derived__newtab_interactions__v1
    )

    telemetry_derived__newtab_daily_interactions_aggregates__v1.set_upstream(
        telemetry_derived__newtab_interactions__v1
    )

    telemetry_derived__newtab_interactions__v1.set_upstream(
        wait_for_checks__fail_telemetry_derived__clients_last_seen__v2
    )

    telemetry_derived__newtab_interactions__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    telemetry_derived__newtab_visits__v1.set_upstream(
        wait_for_checks__fail_telemetry_derived__clients_last_seen__v2
    )

    telemetry_derived__newtab_visits__v1.set_upstream(wait_for_copy_deduplicate_all)
