# Generated via https://github.com/mozilla/bigquery-etl/blob/main/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from operators.task_sensor import ExternalTaskCompletedSensor
import datetime
from utils.gcp import bigquery_etl_query, gke_command

docs = """
### bqetl_fog_decision_support

Built from bigquery-etl repo, [`dags/bqetl_fog_decision_support.py`](https://github.com/mozilla/bigquery-etl/blob/main/dags/bqetl_fog_decision_support.py)

#### Description

This DAG schedules queries that calculate FOG decision support metrics.
#### Owner

pmcmanis@mozilla.com
"""


default_args = {
    "owner": "pmcmanis@mozilla.com",
    "start_date": datetime.datetime(2022, 5, 25, 0, 0),
    "end_date": None,
    "email": ["telemetry-alerts@mozilla.com", "pmcmanis@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
}

tags = ["impact/tier_3", "repo/bigquery-etl"]

with DAG(
    "bqetl_fog_decision_support",
    default_args=default_args,
    schedule_interval="0 4 * * *",
    doc_md=docs,
    tags=tags,
) as dag:

    fog_decision_support_percentiles_v1 = bigquery_etl_query(
        task_id="fog_decision_support_percentiles_v1",
        destination_table="fog_decision_support_percentiles_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="pmcmanis@mozilla.com",
        email=["pmcmanis@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )
