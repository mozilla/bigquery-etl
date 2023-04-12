# Generated via https://github.com/mozilla/bigquery-etl/blob/main/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
import datetime
from utils.gcp import bigquery_etl_query

docs = """
### bqetl_download_funnel_attribution

Built from bigquery-etl repo, [`dags/bqetl_download_funnel_attribution.py`](https://github.com/mozilla/bigquery-etl/blob/main/dags/bqetl_download_funnel_attribution.py)

#### Description

Daily aggregations of data exported from Google Analytics joined with Firefox download data.
#### Owner

gleonard@mozilla.com
"""


default_args = {
    "owner": "gleonard@mozilla.com",
    "start_date": datetime.datetime(2023, 4, 10, 0, 0),
    "end_date": None,
    "email": ["gleonard@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
}

tags = ["impact/tier_1", "repo/bigquery-etl"]

with DAG(
    "bqetl_download_funnel_attribution",
    default_args=default_args,
    schedule_interval="0 23 * * *",
    doc_md=docs,
    tags=tags,
) as dag:
    ga_derived__www_site_empty_check__v1 = bigquery_etl_query(
        task_id="ga_derived__www_site_empty_check__v1",
        destination_table=None,
        dataset_id="ga_derived",
        project_id="moz-fx-data-marketing-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        parameters=["submission_date:DATE:{{ds}}"],
        sql_file_path="sql/moz-fx-data-marketing-prod/ga_derived/www_site_empty_check_v1/query.sql",
    )
    ga_derived__downloads_with_attribution__v2 = bigquery_etl_query(
        task_id="ga_derived__downloads_with_attribution__v2",
        destination_table="downloads_with_attribution_v2",
        dataset_id="ga_derived",
        project_id="moz-fx-data-marketing-prod",
        owner="gleonard@mozilla.com",
        email=["gleonard@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        parameters=["download_date:DATE:{{macros.ds_add(ds, -1)}}"],
    )
    ga_derived__downloads_with_attribution__v2.set_upstream(
        ga_derived__www_site_empty_check__v1
    )
