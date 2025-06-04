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
from utils.gcp import bigquery_etl_query, bigquery_dq_check, bigquery_bigeye_check

docs = """
### bqetl_ga4_firefoxdotcom

Built from bigquery-etl repo, [`dags/bqetl_ga4_firefoxdotcom.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_ga4_firefoxdotcom.py)

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
    "start_date": datetime.datetime(2025, 5, 27, 0, 0),
    "end_date": None,
    "email": ["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "max_active_tis_per_dag": None,
}

tags = ["impact/tier_2", "repo/bigquery-etl"]

with DAG(
    "bqetl_ga4_firefoxdotcom",
    default_args=default_args,
    schedule_interval="0 14 * * *",
    doc_md=docs,
    tags=tags,
    catchup=False,
) as dag:

    wait_for_firefoxdotcom_events_table = BigQueryTableExistenceSensor(
        task_id="wait_for_firefoxdotcom_events_table",
        project_id="moz-fx-data-marketing-prod",
        dataset_id="analytics_489412379",
        table_id="events_{{ ds_nodash }}",
        gcp_conn_id="google_cloud_shared_prod",
        deferrable=True,
        poke_interval=datetime.timedelta(seconds=1800),
        timeout=datetime.timedelta(seconds=36000),
        retries=1,
        retry_delay=datetime.timedelta(seconds=1800),
    )

    checks__fail_firefoxdotcom_derived__ga_clients__v1 = bigquery_dq_check(
        task_id="checks__fail_firefoxdotcom_derived__ga_clients__v1",
        source_table="ga_clients_v1",
        dataset_id="firefoxdotcom_derived",
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

    checks__warn_firefoxdotcom_derived__ga_sessions__v1 = bigquery_dq_check(
        task_id="checks__warn_firefoxdotcom_derived__ga_sessions__v1",
        source_table="ga_sessions_v1",
        dataset_id="firefoxdotcom_derived",
        project_id="moz-fx-data-shared-prod",
        is_dq_check_fail=False,
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        task_concurrency=1,
        parameters=["submission_date:DATE:{{ds}}"],
        retries=0,
    )

    checks__warn_firefoxdotcom_derived__www_site_hits__v1 = bigquery_dq_check(
        task_id="checks__warn_firefoxdotcom_derived__www_site_hits__v1",
        source_table="www_site_hits_v1",
        dataset_id="firefoxdotcom_derived",
        project_id="moz-fx-data-shared-prod",
        is_dq_check_fail=False,
        owner="mhirose@mozilla.com",
        email=[
            "kwindau@mozilla.com",
            "mhirose@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        depends_on_past=False,
        parameters=["submission_date:DATE:{{ds}}"],
        retries=0,
    )

    firefoxdotcom_derived__ga_clients__v1 = bigquery_etl_query(
        task_id="firefoxdotcom_derived__ga_clients__v1",
        destination_table="ga_clients_v1",
        dataset_id="firefoxdotcom_derived",
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

    firefoxdotcom_derived__ga_sessions__v1 = bigquery_etl_query(
        task_id="firefoxdotcom_derived__ga_sessions__v1",
        destination_table=None,
        dataset_id="firefoxdotcom_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        parameters=["submission_date:DATE:{{ds}}"],
        sql_file_path="sql/moz-fx-data-shared-prod/firefoxdotcom_derived/ga_sessions_v1/script.sql",
    )

    firefoxdotcom_derived__www_site_downloads__v1 = bigquery_etl_query(
        task_id="firefoxdotcom_derived__www_site_downloads__v1",
        destination_table="www_site_downloads_v1",
        dataset_id="firefoxdotcom_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    firefoxdotcom_derived__www_site_events_metrics__v1 = bigquery_etl_query(
        task_id="firefoxdotcom_derived__www_site_events_metrics__v1",
        destination_table="www_site_events_metrics_v1",
        dataset_id="firefoxdotcom_derived",
        project_id="moz-fx-data-shared-prod",
        owner="mhirose@mozilla.com",
        email=[
            "kwindau@mozilla.com",
            "mhirose@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    firefoxdotcom_derived__www_site_hits__v1 = bigquery_etl_query(
        task_id="firefoxdotcom_derived__www_site_hits__v1",
        destination_table="www_site_hits_v1",
        dataset_id="firefoxdotcom_derived",
        project_id="moz-fx-data-shared-prod",
        owner="mhirose@mozilla.com",
        email=[
            "kwindau@mozilla.com",
            "mhirose@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    firefoxdotcom_derived__www_site_landing_page_metrics__v1 = bigquery_etl_query(
        task_id="firefoxdotcom_derived__www_site_landing_page_metrics__v1",
        destination_table="www_site_landing_page_metrics_v1",
        dataset_id="firefoxdotcom_derived",
        project_id="moz-fx-data-shared-prod",
        owner="mhirose@mozilla.com",
        email=[
            "kwindau@mozilla.com",
            "mhirose@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    firefoxdotcom_derived__wwww_site_metrics_summary__v1 = bigquery_etl_query(
        task_id="firefoxdotcom_derived__wwww_site_metrics_summary__v1",
        destination_table="wwww_site_metrics_summary_v1",
        dataset_id="firefoxdotcom_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    checks__fail_firefoxdotcom_derived__ga_clients__v1.set_upstream(
        firefoxdotcom_derived__ga_clients__v1
    )

    checks__warn_firefoxdotcom_derived__ga_sessions__v1.set_upstream(
        firefoxdotcom_derived__ga_sessions__v1
    )

    checks__warn_firefoxdotcom_derived__www_site_hits__v1.set_upstream(
        firefoxdotcom_derived__www_site_hits__v1
    )

    firefoxdotcom_derived__ga_clients__v1.set_upstream(
        firefoxdotcom_derived__ga_sessions__v1
    )

    firefoxdotcom_derived__ga_sessions__v1.set_upstream(
        wait_for_firefoxdotcom_events_table
    )

    firefoxdotcom_derived__www_site_downloads__v1.set_upstream(
        wait_for_firefoxdotcom_events_table
    )

    firefoxdotcom_derived__www_site_events_metrics__v1.set_upstream(
        firefoxdotcom_derived__www_site_hits__v1
    )

    firefoxdotcom_derived__www_site_hits__v1.set_upstream(
        wait_for_firefoxdotcom_events_table
    )

    firefoxdotcom_derived__www_site_landing_page_metrics__v1.set_upstream(
        firefoxdotcom_derived__www_site_hits__v1
    )

    firefoxdotcom_derived__wwww_site_metrics_summary__v1.set_upstream(
        wait_for_firefoxdotcom_events_table
    )
