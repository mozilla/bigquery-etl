# Generated via https://github.com/mozilla/bigquery-etl/blob/main/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskMarker
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
import datetime
from utils.constants import ALLOWED_STATES, FAILED_STATES
from utils.gcp import bigquery_etl_query, gke_command, bigquery_dq_check

docs = """
### bqetl_firefox_ios

Built from bigquery-etl repo, [`dags/bqetl_firefox_ios.py`](https://github.com/mozilla/bigquery-etl/blob/main/dags/bqetl_firefox_ios.py)

#### Description

Schedule daily ios firefox ETL
#### Owner

kik@mozilla.com
"""


default_args = {
    "owner": "kik@mozilla.com",
    "start_date": datetime.datetime(2021, 3, 18, 0, 0),
    "end_date": None,
    "email": ["kik@mozilla.com", "frank@mozilla.com", "telemetry-alerts@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
}

tags = ["impact/tier_1", "repo/bigquery-etl"]

with DAG(
    "bqetl_firefox_ios",
    default_args=default_args,
    schedule_interval="0 4 * * *",
    doc_md=docs,
    tags=tags,
) as dag:
    checks__fail_firefox_ios_derived__app_store_funnel__v1 = bigquery_dq_check(
        task_id="checks__fail_firefox_ios_derived__app_store_funnel__v1",
        source_table="app_store_funnel_v1",
        dataset_id="firefox_ios_derived",
        project_id="moz-fx-data-shared-prod",
        is_dq_check_fail=True,
        owner="kik@mozilla.com",
        email=["frank@mozilla.com", "kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        parameters=["submission_date:DATE:{{ds}}"],
        retries=0,
    )

    checks__fail_firefox_ios_derived__baseline_clients_yearly__v1 = bigquery_dq_check(
        task_id="checks__fail_firefox_ios_derived__baseline_clients_yearly__v1",
        source_table="baseline_clients_yearly_v1",
        dataset_id="firefox_ios_derived",
        project_id="moz-fx-data-shared-prod",
        is_dq_check_fail=True,
        owner="frank@mozilla.com",
        email=["frank@mozilla.com", "kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        parameters=["submission_date:DATE:{{ds}}"],
        retries=0,
    )

    checks__fail_firefox_ios_derived__client_adclicks_history__v1 = bigquery_dq_check(
        task_id="checks__fail_firefox_ios_derived__client_adclicks_history__v1",
        source_table="client_adclicks_history_v1",
        dataset_id="firefox_ios_derived",
        project_id="moz-fx-data-shared-prod",
        is_dq_check_fail=True,
        owner="frank@mozilla.com",
        email=["frank@mozilla.com", "kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        task_concurrency=1,
        parameters=["submission_date:DATE:{{ds}}"],
        retries=0,
    )

    checks__fail_firefox_ios_derived__clients_activation__v1 = bigquery_dq_check(
        task_id="checks__fail_firefox_ios_derived__clients_activation__v1",
        source_table="clients_activation_v1",
        dataset_id="firefox_ios_derived",
        project_id="moz-fx-data-shared-prod",
        is_dq_check_fail=True,
        owner="vsabino@mozilla.com",
        email=[
            "frank@mozilla.com",
            "kik@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "vsabino@mozilla.com",
        ],
        depends_on_past=False,
        parameters=["submission_date:DATE:{{ds}}"],
        retries=0,
    )

    checks__fail_firefox_ios_derived__firefox_ios_clients__v1 = bigquery_dq_check(
        task_id="checks__fail_firefox_ios_derived__firefox_ios_clients__v1",
        source_table="firefox_ios_clients_v1",
        dataset_id="firefox_ios_derived",
        project_id="moz-fx-data-shared-prod",
        is_dq_check_fail=True,
        owner="kik@mozilla.com",
        email=["frank@mozilla.com", "kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        task_concurrency=1,
        parameters=["submission_date:DATE:{{ds}}"],
        retries=0,
    )

    with TaskGroup(
        "checks__fail_firefox_ios_derived__firefox_ios_clients__v1_external",
    ) as checks__fail_firefox_ios_derived__firefox_ios_clients__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_analytics_aggregations__wait_for_checks__fail_firefox_ios_derived__firefox_ios_clients__v1",
            external_dag_id="bqetl_analytics_aggregations",
            external_task_id="wait_for_checks__fail_firefox_ios_derived__firefox_ios_clients__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=85500)).isoformat() }}",
        )

        checks__fail_firefox_ios_derived__firefox_ios_clients__v1_external.set_upstream(
            checks__fail_firefox_ios_derived__firefox_ios_clients__v1
        )

    checks__fail_firefox_ios_derived__funnel_retention_clients_week_2__v1 = bigquery_dq_check(
        task_id="checks__fail_firefox_ios_derived__funnel_retention_clients_week_2__v1",
        source_table="funnel_retention_clients_week_2_v1",
        dataset_id="firefox_ios_derived",
        project_id="moz-fx-data-shared-prod",
        is_dq_check_fail=True,
        owner="kik@mozilla.com",
        email=["frank@mozilla.com", "kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        parameters=["submission_date:DATE:{{ds}}"],
        retries=0,
    )

    checks__fail_firefox_ios_derived__funnel_retention_clients_week_4__v1 = bigquery_dq_check(
        task_id="checks__fail_firefox_ios_derived__funnel_retention_clients_week_4__v1",
        source_table="funnel_retention_clients_week_4_v1",
        dataset_id="firefox_ios_derived",
        project_id="moz-fx-data-shared-prod",
        is_dq_check_fail=True,
        owner="kik@mozilla.com",
        email=["frank@mozilla.com", "kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        parameters=["submission_date:DATE:{{ds}}"],
        retries=0,
    )

    checks__fail_firefox_ios_derived__funnel_retention_week_4__v1 = bigquery_dq_check(
        task_id="checks__fail_firefox_ios_derived__funnel_retention_week_4__v1",
        source_table="funnel_retention_week_4_v1",
        dataset_id="firefox_ios_derived",
        project_id="moz-fx-data-shared-prod",
        is_dq_check_fail=True,
        owner="kik@mozilla.com",
        email=["frank@mozilla.com", "kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        parameters=["submission_date:DATE:{{ds}}"],
        retries=0,
    )

    checks__fail_firefox_ios_derived__new_profile_activation__v2 = bigquery_dq_check(
        task_id="checks__fail_firefox_ios_derived__new_profile_activation__v2",
        source_table="new_profile_activation_v2",
        dataset_id="firefox_ios_derived",
        project_id="moz-fx-data-shared-prod",
        is_dq_check_fail=True,
        owner="vsabino@mozilla.com",
        email=[
            "frank@mozilla.com",
            "kik@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "vsabino@mozilla.com",
        ],
        depends_on_past=False,
        parameters=["submission_date:DATE:{{ds}}"],
        retries=0,
    )

    checks__warn_firefox_ios_derived__app_store_funnel__v1 = bigquery_dq_check(
        task_id="checks__warn_firefox_ios_derived__app_store_funnel__v1",
        source_table="app_store_funnel_v1",
        dataset_id="firefox_ios_derived",
        project_id="moz-fx-data-shared-prod",
        is_dq_check_fail=False,
        owner="kik@mozilla.com",
        email=["frank@mozilla.com", "kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        parameters=["submission_date:DATE:{{ds}}"],
        retries=0,
    )

    checks__warn_firefox_ios_derived__firefox_ios_clients__v1 = bigquery_dq_check(
        task_id="checks__warn_firefox_ios_derived__firefox_ios_clients__v1",
        source_table="firefox_ios_clients_v1",
        dataset_id="firefox_ios_derived",
        project_id="moz-fx-data-shared-prod",
        is_dq_check_fail=False,
        owner="kik@mozilla.com",
        email=["frank@mozilla.com", "kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        task_concurrency=1,
        parameters=["submission_date:DATE:{{ds}}"],
        retries=0,
    )

    firefox_ios_derived__app_store_funnel__v1 = bigquery_etl_query(
        task_id="firefox_ios_derived__app_store_funnel__v1",
        destination_table="app_store_funnel_v1",
        dataset_id="firefox_ios_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kik@mozilla.com",
        email=["frank@mozilla.com", "kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    firefox_ios_derived__attributable_clients__v1 = bigquery_etl_query(
        task_id="firefox_ios_derived__attributable_clients__v1",
        destination_table="attributable_clients_v1",
        dataset_id="firefox_ios_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kik@mozilla.com",
        email=["frank@mozilla.com", "kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    firefox_ios_derived__baseline_clients_yearly__v1 = bigquery_etl_query(
        task_id="firefox_ios_derived__baseline_clients_yearly__v1",
        destination_table="baseline_clients_yearly_v1",
        dataset_id="firefox_ios_derived",
        project_id="moz-fx-data-shared-prod",
        owner="frank@mozilla.com",
        email=["frank@mozilla.com", "kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=True,
    )

    firefox_ios_derived__client_adclicks_history__v1 = bigquery_etl_query(
        task_id="firefox_ios_derived__client_adclicks_history__v1",
        destination_table="client_adclicks_history_v1",
        dataset_id="firefox_ios_derived",
        project_id="moz-fx-data-shared-prod",
        owner="frank@mozilla.com",
        email=["frank@mozilla.com", "kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
        parameters=["submission_date:DATE:{{ds}}"],
    )

    firefox_ios_derived__clients_activation__v1 = bigquery_etl_query(
        task_id="firefox_ios_derived__clients_activation__v1",
        destination_table="clients_activation_v1",
        dataset_id="firefox_ios_derived",
        project_id="moz-fx-data-shared-prod",
        owner="vsabino@mozilla.com",
        email=[
            "frank@mozilla.com",
            "kik@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "vsabino@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    firefox_ios_derived__firefox_ios_clients__v1 = bigquery_etl_query(
        task_id="firefox_ios_derived__firefox_ios_clients__v1",
        destination_table="firefox_ios_clients_v1",
        dataset_id="firefox_ios_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kik@mozilla.com",
        email=["frank@mozilla.com", "kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=True,
        parameters=["submission_date:DATE:{{ds}}"],
    )

    firefox_ios_derived__funnel_retention_clients_week_2__v1 = bigquery_etl_query(
        task_id="firefox_ios_derived__funnel_retention_clients_week_2__v1",
        destination_table="funnel_retention_clients_week_2_v1",
        dataset_id="firefox_ios_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kik@mozilla.com",
        email=["frank@mozilla.com", "kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    firefox_ios_derived__funnel_retention_clients_week_4__v1 = bigquery_etl_query(
        task_id="firefox_ios_derived__funnel_retention_clients_week_4__v1",
        destination_table="funnel_retention_clients_week_4_v1",
        dataset_id="firefox_ios_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kik@mozilla.com",
        email=["frank@mozilla.com", "kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    firefox_ios_derived__funnel_retention_week_4__v1 = bigquery_etl_query(
        task_id="firefox_ios_derived__funnel_retention_week_4__v1",
        destination_table="funnel_retention_week_4_v1",
        dataset_id="firefox_ios_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kik@mozilla.com",
        email=["frank@mozilla.com", "kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    firefox_ios_derived__new_profile_activation__v2 = bigquery_etl_query(
        task_id="firefox_ios_derived__new_profile_activation__v2",
        destination_table="new_profile_activation_v2",
        dataset_id="firefox_ios_derived",
        project_id="moz-fx-data-shared-prod",
        owner="vsabino@mozilla.com",
        email=[
            "frank@mozilla.com",
            "kik@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "vsabino@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=True,
    )

    org_mozilla_ios_firefox__unified_metrics__v1 = gke_command(
        task_id="org_mozilla_ios_firefox__unified_metrics__v1",
        command=[
            "python",
            "sql/moz-fx-data-shared-prod/org_mozilla_ios_firefox/unified_metrics_v1/query.py",
        ]
        + [],
        docker_image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
        owner="kik@mozilla.com",
        email=["frank@mozilla.com", "kik@mozilla.com", "telemetry-alerts@mozilla.com"],
    )

    checks__fail_firefox_ios_derived__app_store_funnel__v1.set_upstream(
        firefox_ios_derived__app_store_funnel__v1
    )

    checks__fail_firefox_ios_derived__baseline_clients_yearly__v1.set_upstream(
        firefox_ios_derived__baseline_clients_yearly__v1
    )

    checks__fail_firefox_ios_derived__client_adclicks_history__v1.set_upstream(
        firefox_ios_derived__client_adclicks_history__v1
    )

    checks__fail_firefox_ios_derived__clients_activation__v1.set_upstream(
        firefox_ios_derived__clients_activation__v1
    )

    checks__fail_firefox_ios_derived__firefox_ios_clients__v1.set_upstream(
        firefox_ios_derived__firefox_ios_clients__v1
    )

    checks__fail_firefox_ios_derived__funnel_retention_clients_week_2__v1.set_upstream(
        firefox_ios_derived__funnel_retention_clients_week_2__v1
    )

    checks__fail_firefox_ios_derived__funnel_retention_clients_week_4__v1.set_upstream(
        firefox_ios_derived__funnel_retention_clients_week_4__v1
    )

    checks__fail_firefox_ios_derived__funnel_retention_week_4__v1.set_upstream(
        firefox_ios_derived__funnel_retention_week_4__v1
    )

    checks__fail_firefox_ios_derived__new_profile_activation__v2.set_upstream(
        firefox_ios_derived__new_profile_activation__v2
    )

    checks__warn_firefox_ios_derived__app_store_funnel__v1.set_upstream(
        firefox_ios_derived__app_store_funnel__v1
    )

    checks__warn_firefox_ios_derived__firefox_ios_clients__v1.set_upstream(
        firefox_ios_derived__firefox_ios_clients__v1
    )

    wait_for_app_store_external__firefox_app_store_territory_source_type_report__v1 = ExternalTaskSensor(
        task_id="wait_for_app_store_external__firefox_app_store_territory_source_type_report__v1",
        external_dag_id="bqetl_fivetran_copied_tables",
        external_task_id="app_store_external__firefox_app_store_territory_source_type_report__v1",
        execution_delta=datetime.timedelta(seconds=3600),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    firefox_ios_derived__app_store_funnel__v1.set_upstream(
        wait_for_app_store_external__firefox_app_store_territory_source_type_report__v1
    )
    wait_for_app_store_external__firefox_downloads_territory_source_type_report__v1 = ExternalTaskSensor(
        task_id="wait_for_app_store_external__firefox_downloads_territory_source_type_report__v1",
        external_dag_id="bqetl_fivetran_copied_tables",
        external_task_id="app_store_external__firefox_downloads_territory_source_type_report__v1",
        execution_delta=datetime.timedelta(seconds=3600),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    firefox_ios_derived__app_store_funnel__v1.set_upstream(
        wait_for_app_store_external__firefox_downloads_territory_source_type_report__v1
    )

    firefox_ios_derived__app_store_funnel__v1.set_upstream(
        checks__fail_firefox_ios_derived__firefox_ios_clients__v1
    )

    firefox_ios_derived__attributable_clients__v1.set_upstream(
        checks__fail_firefox_ios_derived__firefox_ios_clients__v1
    )
    wait_for_copy_deduplicate_all = ExternalTaskSensor(
        task_id="wait_for_copy_deduplicate_all",
        external_dag_id="copy_deduplicate",
        external_task_id="copy_deduplicate_all",
        execution_delta=datetime.timedelta(seconds=10800),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    firefox_ios_derived__attributable_clients__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )
    wait_for_search_derived__mobile_search_clients_daily__v1 = ExternalTaskSensor(
        task_id="wait_for_search_derived__mobile_search_clients_daily__v1",
        external_dag_id="bqetl_mobile_search",
        external_task_id="search_derived__mobile_search_clients_daily__v1",
        execution_delta=datetime.timedelta(seconds=7200),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    firefox_ios_derived__attributable_clients__v1.set_upstream(
        wait_for_search_derived__mobile_search_clients_daily__v1
    )

    wait_for_checks__fail_org_mozilla_ios_fennec_derived__baseline_clients_daily__v1 = ExternalTaskSensor(
        task_id="wait_for_checks__fail_org_mozilla_ios_fennec_derived__baseline_clients_daily__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="firefox_ios.checks__fail_org_mozilla_ios_fennec_derived__baseline_clients_daily__v1",
        execution_delta=datetime.timedelta(seconds=7200),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    firefox_ios_derived__baseline_clients_yearly__v1.set_upstream(
        wait_for_checks__fail_org_mozilla_ios_fennec_derived__baseline_clients_daily__v1
    )
    wait_for_checks__fail_org_mozilla_ios_firefox_derived__baseline_clients_daily__v1 = ExternalTaskSensor(
        task_id="wait_for_checks__fail_org_mozilla_ios_firefox_derived__baseline_clients_daily__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="firefox_ios.checks__fail_org_mozilla_ios_firefox_derived__baseline_clients_daily__v1",
        execution_delta=datetime.timedelta(seconds=7200),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    firefox_ios_derived__baseline_clients_yearly__v1.set_upstream(
        wait_for_checks__fail_org_mozilla_ios_firefox_derived__baseline_clients_daily__v1
    )
    wait_for_checks__fail_org_mozilla_ios_firefoxbeta_derived__baseline_clients_daily__v1 = ExternalTaskSensor(
        task_id="wait_for_checks__fail_org_mozilla_ios_firefoxbeta_derived__baseline_clients_daily__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="firefox_ios.checks__fail_org_mozilla_ios_firefoxbeta_derived__baseline_clients_daily__v1",
        execution_delta=datetime.timedelta(seconds=7200),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    firefox_ios_derived__baseline_clients_yearly__v1.set_upstream(
        wait_for_checks__fail_org_mozilla_ios_firefoxbeta_derived__baseline_clients_daily__v1
    )

    firefox_ios_derived__client_adclicks_history__v1.set_upstream(
        firefox_ios_derived__attributable_clients__v1
    )

    firefox_ios_derived__clients_activation__v1.set_upstream(
        checks__fail_firefox_ios_derived__firefox_ios_clients__v1
    )
    wait_for_org_mozilla_ios_fennec_derived__baseline_clients_last_seen__v1 = ExternalTaskSensor(
        task_id="wait_for_org_mozilla_ios_fennec_derived__baseline_clients_last_seen__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="firefox_ios.org_mozilla_ios_fennec_derived__baseline_clients_last_seen__v1",
        execution_delta=datetime.timedelta(seconds=7200),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    firefox_ios_derived__clients_activation__v1.set_upstream(
        wait_for_org_mozilla_ios_fennec_derived__baseline_clients_last_seen__v1
    )
    wait_for_org_mozilla_ios_firefox_derived__baseline_clients_last_seen__v1 = ExternalTaskSensor(
        task_id="wait_for_org_mozilla_ios_firefox_derived__baseline_clients_last_seen__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="firefox_ios.org_mozilla_ios_firefox_derived__baseline_clients_last_seen__v1",
        execution_delta=datetime.timedelta(seconds=7200),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    firefox_ios_derived__clients_activation__v1.set_upstream(
        wait_for_org_mozilla_ios_firefox_derived__baseline_clients_last_seen__v1
    )
    wait_for_org_mozilla_ios_firefoxbeta_derived__baseline_clients_last_seen__v1 = ExternalTaskSensor(
        task_id="wait_for_org_mozilla_ios_firefoxbeta_derived__baseline_clients_last_seen__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="firefox_ios.org_mozilla_ios_firefoxbeta_derived__baseline_clients_last_seen__v1",
        execution_delta=datetime.timedelta(seconds=7200),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    firefox_ios_derived__clients_activation__v1.set_upstream(
        wait_for_org_mozilla_ios_firefoxbeta_derived__baseline_clients_last_seen__v1
    )
    firefox_ios_derived__clients_activation__v1.set_upstream(
        wait_for_search_derived__mobile_search_clients_daily__v1
    )

    firefox_ios_derived__firefox_ios_clients__v1.set_upstream(
        checks__fail_firefox_ios_derived__new_profile_activation__v2
    )
    firefox_ios_derived__firefox_ios_clients__v1.set_upstream(
        wait_for_checks__fail_org_mozilla_ios_fennec_derived__baseline_clients_daily__v1
    )
    firefox_ios_derived__firefox_ios_clients__v1.set_upstream(
        wait_for_checks__fail_org_mozilla_ios_firefox_derived__baseline_clients_daily__v1
    )
    firefox_ios_derived__firefox_ios_clients__v1.set_upstream(
        wait_for_checks__fail_org_mozilla_ios_firefoxbeta_derived__baseline_clients_daily__v1
    )
    firefox_ios_derived__firefox_ios_clients__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    firefox_ios_derived__funnel_retention_clients_week_2__v1.set_upstream(
        checks__fail_firefox_ios_derived__firefox_ios_clients__v1
    )
    firefox_ios_derived__funnel_retention_clients_week_2__v1.set_upstream(
        wait_for_org_mozilla_ios_fennec_derived__baseline_clients_last_seen__v1
    )
    firefox_ios_derived__funnel_retention_clients_week_2__v1.set_upstream(
        wait_for_org_mozilla_ios_firefox_derived__baseline_clients_last_seen__v1
    )
    firefox_ios_derived__funnel_retention_clients_week_2__v1.set_upstream(
        wait_for_org_mozilla_ios_firefoxbeta_derived__baseline_clients_last_seen__v1
    )

    firefox_ios_derived__funnel_retention_week_4__v1.set_upstream(
        checks__fail_firefox_ios_derived__funnel_retention_clients_week_4__v1
    )

    firefox_ios_derived__new_profile_activation__v2.set_upstream(
        wait_for_org_mozilla_ios_fennec_derived__baseline_clients_last_seen__v1
    )
    firefox_ios_derived__new_profile_activation__v2.set_upstream(
        wait_for_org_mozilla_ios_firefox_derived__baseline_clients_last_seen__v1
    )
    firefox_ios_derived__new_profile_activation__v2.set_upstream(
        wait_for_org_mozilla_ios_firefoxbeta_derived__baseline_clients_last_seen__v1
    )
    firefox_ios_derived__new_profile_activation__v2.set_upstream(
        wait_for_search_derived__mobile_search_clients_daily__v1
    )
