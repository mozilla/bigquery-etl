# Generated via https://github.com/mozilla/bigquery-etl/blob/main/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from airflow.providers.google.cloud.sensors.bigquery import (
    BigQueryTableExistenceSensor,
    BigQueryTablePartitionExistenceSensor,
)
from airflow.sensors.external_task import ExternalTaskMarker
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
import datetime
from operators.gcp_container_operator import GKEPodOperator
from utils.constants import ALLOWED_STATES, FAILED_STATES
from utils.gcp import bigquery_etl_query, bigquery_dq_check

docs = """
### bqetl_google_analytics_derived_ga4

Built from bigquery-etl repo, [`dags/bqetl_google_analytics_derived_ga4.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_google_analytics_derived_ga4.py)

#### Description

Daily aggregations of data exported from Google Analytics 4
#### Owner

kwindau@mozilla.com

#### Tags

* impact/tier_2
* repo/bigquery-etl
"""


default_args = {
    "owner": "kwindau@mozilla.com",
    "start_date": datetime.datetime(2024, 1, 3, 0, 0),
    "end_date": None,
    "email": ["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
}

tags = ["impact/tier_2", "repo/bigquery-etl"]

with DAG(
    "bqetl_google_analytics_derived_ga4",
    default_args=default_args,
    schedule_interval="0 12 * * *",
    doc_md=docs,
    tags=tags,
) as dag:

    wait_for_blogs_events_table = BigQueryTableExistenceSensor(
        task_id="wait_for_blogs_events_table",
        project_id="moz-fx-data-marketing-prod",
        dataset_id="analytics_314399816",
        table_id="events_{{ ds_nodash }}",
        gcp_conn_id="google_cloud_shared_prod",
        deferrable=True,
        poke_interval=datetime.timedelta(seconds=1800),
        timeout=datetime.timedelta(seconds=36000),
        retries=1,
        retry_delay=datetime.timedelta(seconds=1800),
    )

    wait_for_wmo_events_table = BigQueryTableExistenceSensor(
        task_id="wait_for_wmo_events_table",
        project_id="moz-fx-data-marketing-prod",
        dataset_id="analytics_313696158",
        table_id="events_{{ ds_nodash }}",
        gcp_conn_id="google_cloud_shared_prod",
        deferrable=True,
        poke_interval=datetime.timedelta(seconds=1800),
        timeout=datetime.timedelta(seconds=36000),
        retries=1,
        retry_delay=datetime.timedelta(seconds=1800),
    )

    wait_for_checks__fail_stub_attribution_service_derived__dl_token_ga_attribution_lookup__v1 = ExternalTaskSensor(
        task_id="wait_for_checks__fail_stub_attribution_service_derived__dl_token_ga_attribution_lookup__v1",
        external_dag_id="bqetl_mozilla_org_derived",
        external_task_id="checks__fail_stub_attribution_service_derived__dl_token_ga_attribution_lookup__v1",
        execution_delta=datetime.timedelta(seconds=36000),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_checks__fail_telemetry_derived__clients_first_seen__v2 = (
        ExternalTaskSensor(
            task_id="wait_for_checks__fail_telemetry_derived__clients_first_seen__v2",
            external_dag_id="bqetl_analytics_tables",
            external_task_id="checks__fail_telemetry_derived__clients_first_seen__v2",
            execution_delta=datetime.timedelta(seconds=36000),
            check_existence=True,
            mode="reschedule",
            allowed_states=ALLOWED_STATES,
            failed_states=FAILED_STATES,
            pool="DATA_ENG_EXTERNALTASKSENSOR",
        )
    )

    wait_for_telemetry_derived__clients_daily__v6 = ExternalTaskSensor(
        task_id="wait_for_telemetry_derived__clients_daily__v6",
        external_dag_id="bqetl_main_summary",
        external_task_id="telemetry_derived__clients_daily__v6",
        execution_delta=datetime.timedelta(seconds=36000),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    checks__fail_mozilla_org_derived__ga_clients__v2 = bigquery_dq_check(
        task_id="checks__fail_mozilla_org_derived__ga_clients__v2",
        source_table="ga_clients_v2",
        dataset_id="mozilla_org_derived",
        project_id="moz-fx-data-shared-prod",
        is_dq_check_fail=True,
        owner="mhirose@mozilla.com",
        email=[
            "kwindau@mozilla.com",
            "mhirose@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        depends_on_past=False,
        task_concurrency=1,
        parameters=["session_date:DATE:{{ds}}"],
        retries=0,
    )

    checks__fail_mozilla_org_derived__gclid_conversions__v2 = bigquery_dq_check(
        task_id="checks__fail_mozilla_org_derived__gclid_conversions__v2",
        source_table="gclid_conversions_v2",
        dataset_id="mozilla_org_derived",
        project_id="moz-fx-data-shared-prod",
        is_dq_check_fail=True,
        owner="mhirose@mozilla.com",
        email=[
            "kwindau@mozilla.com",
            "mhirose@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        depends_on_past=False,
        parameters=["conversion_window:INT64:30"] + ["activity_date:DATE:{{ds}}"],
        retries=0,
    )

    checks__warn_ga_derived__blogs_goals__v2 = bigquery_dq_check(
        task_id="checks__warn_ga_derived__blogs_goals__v2",
        source_table="blogs_goals_v2",
        dataset_id="ga_derived",
        project_id="moz-fx-data-marketing-prod",
        is_dq_check_fail=False,
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        parameters=["submission_date:DATE:{{ds}}"],
        retries=0,
    )

    checks__warn_ga_derived__www_site_hits__v2 = bigquery_dq_check(
        task_id="checks__warn_ga_derived__www_site_hits__v2",
        source_table="www_site_hits_v2",
        dataset_id="ga_derived",
        project_id="moz-fx-data-marketing-prod",
        is_dq_check_fail=False,
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        parameters=["submission_date:DATE:{{ds}}"],
        retries=0,
    )

    checks__warn_mozilla_org_derived__ga_sessions__v2 = bigquery_dq_check(
        task_id="checks__warn_mozilla_org_derived__ga_sessions__v2",
        source_table="ga_sessions_v2",
        dataset_id="mozilla_org_derived",
        project_id="moz-fx-data-shared-prod",
        is_dq_check_fail=False,
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        task_concurrency=1,
        parameters=["submission_date:DATE:{{ds}}"],
        retries=0,
    )

    ga_derived__blogs_daily_summary__v2 = bigquery_etl_query(
        task_id="ga_derived__blogs_daily_summary__v2",
        destination_table="blogs_daily_summary_v2",
        dataset_id="ga_derived",
        project_id="moz-fx-data-marketing-prod",
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    ga_derived__blogs_goals__v2 = bigquery_etl_query(
        task_id="ga_derived__blogs_goals__v2",
        destination_table="blogs_goals_v2",
        dataset_id="ga_derived",
        project_id="moz-fx-data-marketing-prod",
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    ga_derived__blogs_landing_page_summary__v2 = bigquery_etl_query(
        task_id="ga_derived__blogs_landing_page_summary__v2",
        destination_table="blogs_landing_page_summary_v2",
        dataset_id="ga_derived",
        project_id="moz-fx-data-marketing-prod",
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    ga_derived__blogs_sessions__v2 = bigquery_etl_query(
        task_id="ga_derived__blogs_sessions__v2",
        destination_table="blogs_sessions_v2",
        dataset_id="ga_derived",
        project_id="moz-fx-data-marketing-prod",
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    ga_derived__firefox_whatsnew_summary__v2 = bigquery_etl_query(
        task_id="ga_derived__firefox_whatsnew_summary__v2",
        destination_table="firefox_whatsnew_summary_v2",
        dataset_id="ga_derived",
        project_id="moz-fx-data-marketing-prod",
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    ga_derived__www_site_downloads__v2 = bigquery_etl_query(
        task_id="ga_derived__www_site_downloads__v2",
        destination_table="www_site_downloads_v2",
        dataset_id="ga_derived",
        project_id="moz-fx-data-marketing-prod",
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    ga_derived__www_site_events_metrics__v2 = bigquery_etl_query(
        task_id="ga_derived__www_site_events_metrics__v2",
        destination_table="www_site_events_metrics_v2",
        dataset_id="ga_derived",
        project_id="moz-fx-data-marketing-prod",
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    ga_derived__www_site_hits__v2 = bigquery_etl_query(
        task_id="ga_derived__www_site_hits__v2",
        destination_table="www_site_hits_v2",
        dataset_id="ga_derived",
        project_id="moz-fx-data-marketing-prod",
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    ga_derived__www_site_landing_page_metrics__v2 = bigquery_etl_query(
        task_id="ga_derived__www_site_landing_page_metrics__v2",
        destination_table="www_site_landing_page_metrics_v2",
        dataset_id="ga_derived",
        project_id="moz-fx-data-marketing-prod",
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    ga_derived__www_site_metrics_summary__v2 = bigquery_etl_query(
        task_id="ga_derived__www_site_metrics_summary__v2",
        destination_table="www_site_metrics_summary_v2",
        dataset_id="ga_derived",
        project_id="moz-fx-data-marketing-prod",
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    ga_derived__www_site_page_metrics__v2 = bigquery_etl_query(
        task_id="ga_derived__www_site_page_metrics__v2",
        destination_table="www_site_page_metrics_v2",
        dataset_id="ga_derived",
        project_id="moz-fx-data-marketing-prod",
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    mozilla_org_derived__ga_clients__v2 = bigquery_etl_query(
        task_id="mozilla_org_derived__ga_clients__v2",
        destination_table="ga_clients_v2",
        dataset_id="mozilla_org_derived",
        project_id="moz-fx-data-shared-prod",
        owner="mhirose@mozilla.com",
        email=[
            "kwindau@mozilla.com",
            "mhirose@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=True,
        parameters=["session_date:DATE:{{ds}}"],
    )

    mozilla_org_derived__ga_sessions__v2 = bigquery_etl_query(
        task_id="mozilla_org_derived__ga_sessions__v2",
        destination_table=None,
        dataset_id="mozilla_org_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        parameters=["submission_date:DATE:{{ds}}"],
        sql_file_path="sql/moz-fx-data-shared-prod/mozilla_org_derived/ga_sessions_v2/script.sql",
    )

    mozilla_org_derived__gclid_conversions__v2 = bigquery_etl_query(
        task_id="mozilla_org_derived__gclid_conversions__v2",
        destination_table="gclid_conversions_v2",
        dataset_id="mozilla_org_derived",
        project_id="moz-fx-data-shared-prod",
        owner="mhirose@mozilla.com",
        email=[
            "kwindau@mozilla.com",
            "mhirose@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter="activity_date",
        depends_on_past=False,
        parameters=["conversion_window:INT64:30"],
    )

    mozilla_vpn_derived__site_metrics_summary__v2 = bigquery_etl_query(
        task_id="mozilla_vpn_derived__site_metrics_summary__v2",
        destination_table="site_metrics_summary_v2",
        dataset_id="mozilla_vpn_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    checks__fail_mozilla_org_derived__ga_clients__v2.set_upstream(
        mozilla_org_derived__ga_clients__v2
    )

    checks__fail_mozilla_org_derived__gclid_conversions__v2.set_upstream(
        mozilla_org_derived__gclid_conversions__v2
    )

    checks__warn_ga_derived__blogs_goals__v2.set_upstream(ga_derived__blogs_goals__v2)

    checks__warn_ga_derived__www_site_hits__v2.set_upstream(
        ga_derived__www_site_hits__v2
    )

    checks__warn_mozilla_org_derived__ga_sessions__v2.set_upstream(
        mozilla_org_derived__ga_sessions__v2
    )

    ga_derived__blogs_daily_summary__v2.set_upstream(ga_derived__blogs_goals__v2)

    ga_derived__blogs_daily_summary__v2.set_upstream(ga_derived__blogs_sessions__v2)

    ga_derived__blogs_goals__v2.set_upstream(wait_for_blogs_events_table)

    ga_derived__blogs_landing_page_summary__v2.set_upstream(ga_derived__blogs_goals__v2)

    ga_derived__blogs_landing_page_summary__v2.set_upstream(
        ga_derived__blogs_sessions__v2
    )

    ga_derived__blogs_landing_page_summary__v2.set_upstream(wait_for_blogs_events_table)

    ga_derived__blogs_sessions__v2.set_upstream(wait_for_blogs_events_table)

    ga_derived__firefox_whatsnew_summary__v2.set_upstream(ga_derived__www_site_hits__v2)

    ga_derived__www_site_downloads__v2.set_upstream(wait_for_wmo_events_table)

    ga_derived__www_site_events_metrics__v2.set_upstream(ga_derived__www_site_hits__v2)

    ga_derived__www_site_hits__v2.set_upstream(wait_for_wmo_events_table)

    ga_derived__www_site_landing_page_metrics__v2.set_upstream(
        ga_derived__www_site_hits__v2
    )

    ga_derived__www_site_metrics_summary__v2.set_upstream(wait_for_wmo_events_table)

    ga_derived__www_site_page_metrics__v2.set_upstream(ga_derived__www_site_hits__v2)

    mozilla_org_derived__ga_clients__v2.set_upstream(
        mozilla_org_derived__ga_sessions__v2
    )

    mozilla_org_derived__ga_sessions__v2.set_upstream(wait_for_wmo_events_table)

    mozilla_org_derived__gclid_conversions__v2.set_upstream(
        wait_for_checks__fail_stub_attribution_service_derived__dl_token_ga_attribution_lookup__v1
    )

    mozilla_org_derived__gclid_conversions__v2.set_upstream(
        wait_for_checks__fail_telemetry_derived__clients_first_seen__v2
    )

    mozilla_org_derived__gclid_conversions__v2.set_upstream(
        mozilla_org_derived__ga_sessions__v2
    )

    mozilla_org_derived__gclid_conversions__v2.set_upstream(
        wait_for_telemetry_derived__clients_daily__v6
    )

    mozilla_vpn_derived__site_metrics_summary__v2.set_upstream(
        wait_for_wmo_events_table
    )