# Generated via https://github.com/mozilla/bigquery-etl/blob/master/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from airflow.operators.sensors import ExternalTaskSensor
import datetime
from utils.gcp import bigquery_etl_query, gke_command

docs = """
### bqetl_core

Built from bigquery-etl repo, [`dags/bqetl_core.py`](https://github.com/mozilla/bigquery-etl/blob/master/dags/bqetl_core.py)

#### Description

Tables derived from the legacy telemetry `core` ping sent by various mobile applications.
#### Owner

jklukas@mozilla.com
"""


default_args = {
    "owner": "jklukas@mozilla.com",
    "start_date": datetime.datetime(2019, 7, 25, 0, 0),
    "end_date": None,
    "email": ["telemetry-alerts@mozilla.com", "jklukas@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=300),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
}

with DAG(
    "bqetl_core", default_args=default_args, schedule_interval="0 2 * * *", doc_md=docs
) as dag:

    telemetry_derived__core_clients_daily__v1 = bigquery_etl_query(
        task_id="telemetry_derived__core_clients_daily__v1",
        destination_table="core_clients_daily_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jklukas@mozilla.com",
        email=["jklukas@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        priority_weight=75,
        dag=dag,
    )

    telemetry_derived__core_clients_last_seen__v1 = bigquery_etl_query(
        task_id="telemetry_derived__core_clients_last_seen__v1",
        destination_table="core_clients_last_seen_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jklukas@mozilla.com",
        email=["jklukas@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=True,
        priority_weight=70,
        dag=dag,
    )

    wait_for_copy_deduplicate_all = ExternalTaskSensor(
        task_id="wait_for_copy_deduplicate_all",
        external_dag_id="copy_deduplicate",
        external_task_id="copy_deduplicate_all",
        execution_delta=datetime.timedelta(seconds=3600),
        check_existence=True,
        mode="reschedule",
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    telemetry_derived__core_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    telemetry_derived__core_clients_last_seen__v1.set_upstream(
        telemetry_derived__core_clients_daily__v1
    )
