# Generated via https://github.com/mozilla/bigquery-etl/blob/main/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from operators.task_sensor import ExternalTaskCompletedSensor
import datetime
from utils.gcp import bigquery_etl_query, gke_command

docs = """
### bqetl_firefox_ios

Built from bigquery-etl repo, [`dags/bqetl_firefox_ios.py`](https://github.com/mozilla/bigquery-etl/blob/main/dags/bqetl_firefox_ios.py)

#### Description

Schedule daily ios firefox ETL
#### Owner

amiyaguchi@mozilla.com
"""


default_args = {
    "owner": "amiyaguchi@mozilla.com",
    "start_date": datetime.datetime(2021, 3, 18, 0, 0),
    "end_date": None,
    "email": ["amiyaguchi@mozilla.com", "telemetry-alerts@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
}

tags = ["impact/tier_1"]

with DAG(
    "bqetl_firefox_ios",
    default_args=default_args,
    schedule_interval="0 4 * * *",
    doc_md=docs,
    tags=tags,
) as dag:

    org_mozilla_ios_firefox__unified_metrics__v1 = gke_command(
        task_id="org_mozilla_ios_firefox__unified_metrics__v1",
        command=[
            "python",
            "sql/moz-fx-data-shared-prod/org_mozilla_ios_firefox/unified_metrics_v1/query.py",
        ]
        + [],
        docker_image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
        owner="amiyaguchi@mozilla.com",
        email=["amiyaguchi@mozilla.com", "telemetry-alerts@mozilla.com"],
    )
