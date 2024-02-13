# Generated via https://github.com/mozilla/bigquery-etl/blob/main/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskMarker
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
import datetime
from operators.gcp_container_operator import GKEPodOperator
from utils.constants import ALLOWED_STATES, FAILED_STATES
from utils.gcp import bigquery_etl_query, bigquery_dq_check

docs = """
### bqetl_google_search_console

Built from bigquery-etl repo, [`dags/bqetl_google_search_console.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_google_search_console.py)

#### Description

ETLs using data exported from Google Search Console.

The Google Search Console exports for a date typically complete by 08:00 UTC two days after that date,
so these ETLs should generally specify `date_partition_offset: -1` in their scheduling metadata.

#### Owner

srose@mozilla.com

#### Tags

* impact/tier_1
* repo/bigquery-etl
"""


default_args = {
    "owner": "srose@mozilla.com",
    "start_date": datetime.datetime(2024, 1, 26, 0, 0),
    "end_date": None,
    "email": ["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
}

tags = ["impact/tier_1", "repo/bigquery-etl"]

with DAG(
    "bqetl_google_search_console",
    default_args=default_args,
    schedule_interval="0 8 * * *",
    doc_md=docs,
    tags=tags,
) as dag:

    google_search_console_derived__search_impressions_by_page__v2 = bigquery_etl_query(
        task_id="google_search_console_derived__search_impressions_by_page__v2",
        destination_table='search_impressions_by_page_v2${{ macros.ds_format(macros.ds_add(ds, -1), "%Y-%m-%d", "%Y%m%d") }}',
        dataset_id="google_search_console_derived",
        project_id="moz-fx-data-marketing-prod",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        parameters=["date:DATE:{{macros.ds_add(ds, -1)}}"],
    )

    google_search_console_derived__search_impressions_by_site__v2 = bigquery_etl_query(
        task_id="google_search_console_derived__search_impressions_by_site__v2",
        destination_table='search_impressions_by_site_v2${{ macros.ds_format(macros.ds_add(ds, -1), "%Y-%m-%d", "%Y%m%d") }}',
        dataset_id="google_search_console_derived",
        project_id="moz-fx-data-marketing-prod",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        parameters=["date:DATE:{{macros.ds_add(ds, -1)}}"],
    )

    google_search_console_derived__site_impression_exports_empty_check__v1 = bigquery_etl_query(
        task_id="google_search_console_derived__site_impression_exports_empty_check__v1",
        destination_table=None,
        dataset_id="google_search_console_derived",
        project_id="moz-fx-data-marketing-prod",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        parameters=["date:DATE:{{macros.ds_add(ds, -1)}}"],
        sql_file_path="sql/moz-fx-data-marketing-prod/google_search_console_derived/site_impression_exports_empty_check_v1/query.sql",
        retry_delay=datetime.timedelta(seconds=3600),
        retries=23,
        email_on_retry=False,
    )

    google_search_console_derived__url_impression_exports_empty_check__v1 = bigquery_etl_query(
        task_id="google_search_console_derived__url_impression_exports_empty_check__v1",
        destination_table=None,
        dataset_id="google_search_console_derived",
        project_id="moz-fx-data-marketing-prod",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        parameters=["date:DATE:{{macros.ds_add(ds, -1)}}"],
        sql_file_path="sql/moz-fx-data-marketing-prod/google_search_console_derived/url_impression_exports_empty_check_v1/query.sql",
        retry_delay=datetime.timedelta(seconds=3600),
        retries=23,
        email_on_retry=False,
    )

    google_search_console_derived__search_impressions_by_page__v2.set_upstream(
        google_search_console_derived__url_impression_exports_empty_check__v1
    )

    google_search_console_derived__search_impressions_by_site__v2.set_upstream(
        google_search_console_derived__site_impression_exports_empty_check__v1
    )
