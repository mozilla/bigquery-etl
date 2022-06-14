# Generated via https://github.com/mozilla/bigquery-etl/blob/main/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskMarker
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
import datetime
from utils.gcp import bigquery_etl_query, gke_command

docs = """
### bqetl_app_store_connect

Built from bigquery-etl repo, [`dags/bqetl_app_store_connect.py`](https://github.com/mozilla/bigquery-etl/blob/main/dags/bqetl_app_store_connect.py)

#### Description

Reports from Apple's App Store Connect API.

Reports are based on Pacific Standard Time (PST). A day includes
transactions that happened from 12:00 a.m. to 11:59 p.m. PST.
https://help.apple.com/app-store-connect/#/dev061699fdb

Daily reports for the Americas are available by 5 am Pacific Time, which can
be either 12:00 or 13:00 UTC, depending on Daylight Savings Time. Daily
reports for Japan, Australia, and New Zealand by 5 am Japan Standard Time;
and 5 am Central European Time for all other territories.

Based on [bug 1720767](https://bugzilla.mozilla.org/show_bug.cgi?id=1720767)
imports have been adjusted to 20:00 UTC.

#### Owner

dthorn@mozilla.com
"""


default_args = {
    "owner": "dthorn@mozilla.com",
    "start_date": datetime.datetime(2021, 1, 15, 0, 0),
    "end_date": None,
    "email": [],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
}

tags = ["impact/tier_3", "repo/bigquery-etl"]

with DAG(
    "bqetl_app_store_connect",
    default_args=default_args,
    schedule_interval="0 20 * * *",
    doc_md=docs,
    tags=tags,
) as dag:

    apple_app_store__report_subscriber_detailed__v13 = gke_command(
        task_id="apple_app_store__report_subscriber_detailed__v13",
        command=[
            "python",
            "sql/moz-fx-data-marketing-prod/apple_app_store/report_subscriber_detailed_v13/query.py",
        ]
        + [
            "--key-id",
            "{{ var.value.app_store_connect_key_id }}",
            "--issuer-id",
            "{{ var.value.app_store_connect_issuer_id }}",
            "--private-key",
            "{{ var.value.app_store_connect_private_key }}",
            "--vendor-number",
            "{{ var.value.app_store_connect_vendor_number }}",
            "--date",
            "{{ ds }}",
            "--table",
            "moz-fx-data-marketing-prod.apple_app_store.report_subscriber_detailed_v13",
        ],
        docker_image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
        owner="dthorn@mozilla.com",
        email=["dthorn@mozilla.com"],
    )
