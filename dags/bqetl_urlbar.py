# Generated via https://github.com/mozilla/bigquery-etl/blob/main/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from operators.task_sensor import ExternalTaskCompletedSensor
import datetime
from utils.gcp import bigquery_etl_query, gke_command

docs = """
### bqetl_urlbar

Built from bigquery-etl repo, [`dags/bqetl_urlbar.py`](https://github.com/mozilla/bigquery-etl/blob/main/dags/bqetl_urlbar.py)

#### Description

Daily aggregation of metrics related to urlbar usage.

#### Owner

anicholson@mozilla.com
"""


default_args = {
    "owner": "anicholson@mozilla.com",
    "start_date": datetime.datetime(2021, 8, 1, 0, 0),
    "end_date": None,
    "email": [
        "telemetry-alerts@mozilla.com",
        "anicholson@mozilla.com",
        "akomar@mozilla.com",
        "tbrooks@mozilla.com",
    ],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
}

with DAG(
    "bqetl_urlbar",
    default_args=default_args,
    schedule_interval="0 3 * * *",
    doc_md=docs,
) as dag:

    telemetry_derived__urlbar_clients_daily__v1 = bigquery_etl_query(
        task_id="telemetry_derived__urlbar_clients_daily__v1",
        destination_table="urlbar_clients_daily_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="anicholson@mozilla.com",
        email=[
            "akomar@mozilla.com",
            "anicholson@mozilla.com",
            "tbrooks@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        dag=dag,
    )

    wait_for_telemetry_derived__clients_daily_joined__v1 = ExternalTaskCompletedSensor(
        task_id="wait_for_telemetry_derived__clients_daily_joined__v1",
        external_dag_id="bqetl_main_summary",
        external_task_id="telemetry_derived__clients_daily_joined__v1",
        execution_delta=datetime.timedelta(seconds=3600),
        check_existence=True,
        mode="reschedule",
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    telemetry_derived__urlbar_clients_daily__v1.set_upstream(
        wait_for_telemetry_derived__clients_daily_joined__v1
    )
