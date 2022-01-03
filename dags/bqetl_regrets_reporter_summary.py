# Generated via https://github.com/mozilla/bigquery-etl/blob/main/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from operators.task_sensor import ExternalTaskCompletedSensor
import datetime
from utils.gcp import bigquery_etl_query, gke_command

docs = """
### bqetl_regrets_reporter_summary

Built from bigquery-etl repo, [`dags/bqetl_regrets_reporter_summary.py`](https://github.com/mozilla/bigquery-etl/blob/main/dags/bqetl_regrets_reporter_summary.py)

#### Description

Measure usage of the regrets reporter addon
#### Owner

kignasiak@mozilla.com
"""


default_args = {
    "owner": "kignasiak@mozilla.com",
    "start_date": datetime.datetime(2021, 12, 12, 0, 0),
    "end_date": None,
    "email": ["telemetry-alerts@mozilla.com", "kignasiak@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
}

tags = ["impact/tier_1", "repo/bigquery-etl"]

with DAG(
    "bqetl_regrets_reporter_summary",
    default_args=default_args,
    schedule_interval="0 4 * * *",
    doc_md=docs,
    tags=tags,
) as dag:

    regrets_reporter_summary__v1 = bigquery_etl_query(
        task_id="regrets_reporter_summary__v1",
        destination_table="regrets_reporter_summary_v1",
        dataset_id="regrets_reporter_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kignasiak@mozilla.com",
        email=["kignasiak@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        dag=dag,
    )
