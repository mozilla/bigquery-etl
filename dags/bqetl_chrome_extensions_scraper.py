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
### bqetl_chrome_extensions_scraper

Built from bigquery-etl repo, [`dags/bqetl_chrome_extensions_scraper.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_chrome_extensions_scraper.py)

#### Description

Pulls info about Chrome extensions from the Chrome webstore
#### Owner

kwindau@mozilla.com

#### Tags

* impact/tier_3
* repo/bigquery-etl
"""


default_args = {
    "owner": "kwindau@mozilla.com",
    "start_date": datetime.datetime(2025, 3, 1, 0, 0),
    "end_date": None,
    "email": ["kwindau@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
}

tags = ["impact/tier_3", "repo/bigquery-etl"]

with DAG(
    "bqetl_chrome_extensions_scraper",
    default_args=default_args,
    schedule_interval="40 21 * * *",
    doc_md=docs,
    tags=tags,
    catchup=False,
) as dag:

    external_derived__chrome_extensions__v1 = GKEPodOperator(
        task_id="external_derived__chrome_extensions__v1",
        arguments=[
            "python",
            "sql/moz-fx-data-shared-prod/external_derived/chrome_extensions_v1/query.py",
        ]
        + ["--date", "{{ds}}"],
        image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com"],
    )
