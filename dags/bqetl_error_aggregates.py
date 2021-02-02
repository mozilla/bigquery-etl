# Generated via https://github.com/mozilla/bigquery-etl/blob/master/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from airflow.operators.sensors import ExternalTaskSensor
import datetime
from utils.gcp import bigquery_etl_query, gke_command

docs = """
### bqetl_error_aggregates

Built from bigquery-etl repo, [`dags/bqetl_error_aggregates.py`](https://github.com/mozilla/bigquery-etl/blob/master/dags/bqetl_error_aggregates.py)

#### Owner

bewu@mozilla.com
"""


default_args = {
    "owner": "bewu@mozilla.com",
    "start_date": datetime.datetime(2019, 11, 1, 0, 0),
    "end_date": None,
    "email": [
        "telemetry-alerts@mozilla.com",
        "bewu@mozilla.com",
        "wlachance@mozilla.com",
    ],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1200),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
}

with DAG(
    "bqetl_error_aggregates",
    default_args=default_args,
    schedule_interval=datetime.timedelta(seconds=10800),
    doc_md=docs,
) as dag:

    telemetry_derived__error_aggregates__v1 = bigquery_etl_query(
        task_id="telemetry_derived__error_aggregates__v1",
        destination_table="error_aggregates_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="bewu@mozilla.com",
        email=[
            "bewu@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wlachance@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        dag=dag,
    )
