# Generated via https://github.com/mozilla/bigquery-etl/blob/main/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskMarker
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
import datetime
from utils.constants import ALLOWED_STATES, FAILED_STATES
from utils.gcp import bigquery_etl_query, gke_command, bigquery_dq_check

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
