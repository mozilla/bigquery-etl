# Generated via https://github.com/mozilla/bigquery-etl/blob/master/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from airflow.operators.sensors import ExternalTaskSensor
import datetime
from utils.gcp import bigquery_etl_query, gke_command

docs = """
### bqetl_pocket

Built from bigquery-etl repo, [`dags/bqetl_pocket.py`](https://github.com/mozilla/bigquery-etl/blob/master/dags/bqetl_pocket.py)

#### Description

Import of data from Pocket's Snowflake warehouse.

Originally created for [Bug 1695336](
https://bugzilla.mozilla.org/show_bug.cgi?id=1695336).

#### Owner

jklukas@mozilla.com
"""


default_args = {
    "owner": "jklukas@mozilla.com",
    "start_date": datetime.datetime(2021, 3, 10, 0, 0),
    "end_date": None,
    "email": ["jklukas@mozilla.com", "telemetry-alerts@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
}

with DAG(
    "bqetl_pocket",
    default_args=default_args,
    schedule_interval="0 12 * * *",
    doc_md=docs,
) as dag:

    pocket_derived__rolling_monthly_active_user_counts__v1 = bigquery_etl_query(
        task_id="pocket_derived__rolling_monthly_active_user_counts__v1",
        destination_table="rolling_monthly_active_user_counts_v1",
        dataset_id="pocket_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jklukas@mozilla.com",
        email=["jklukas@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        parameters=["submission_date:DATE:{{ds}}"],
        dag=dag,
    )

    pocket_derived__rolling_monthly_active_user_counts_history__v1 = gke_command(
        task_id="pocket_derived__rolling_monthly_active_user_counts_history__v1",
        command=[
            "python",
            "sql/moz-fx-data-shared-prod/pocket_derived/rolling_monthly_active_user_counts_history_v1/query.py",
        ]
        + ["--date", "{{ ds }}"],
        docker_image="mozilla/bigquery-etl:latest",
        owner="jklukas@mozilla.com",
        email=["jklukas@mozilla.com", "telemetry-alerts@mozilla.com"],
    )

    pocket_derived__rolling_monthly_active_user_counts__v1.set_upstream(
        pocket_derived__rolling_monthly_active_user_counts_history__v1
    )
