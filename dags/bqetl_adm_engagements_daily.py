# Generated via https://github.com/mozilla/bigquery-etl/blob/main/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from airflow.operators.sensors import ExternalTaskSensor
import datetime
from utils.gcp import bigquery_etl_query, gke_command

docs = """
### bqetl_adm_engagements_daily

Built from bigquery-etl repo, [`dags/bqetl_adm_engagements_daily.py`](https://github.com/mozilla/bigquery-etl/blob/main/dags/bqetl_adm_engagements_daily.py)

#### Description

adMarketplace engagement queries
#### Owner

akomar@mozilla.com
"""


default_args = {
    "owner": "akomar@mozilla.com",
    "start_date": datetime.datetime(2020, 1, 1, 0, 0),
    "end_date": None,
    "email": ["akomar@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
}

with DAG(
    "bqetl_adm_engagements_daily",
    default_args=default_args,
    schedule_interval="0 4 * * *",
    doc_md=docs,
) as dag:

    telemetry_derived__adm_engagements_daily__v1 = bigquery_etl_query(
        task_id="telemetry_derived__adm_engagements_daily__v1",
        destination_table="adm_engagements_daily_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="akomar@mozilla.com",
        email=["akomar@mozilla.com", "rharter@mozilla.com", "tbrooks@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        dag=dag,
    )

    wait_for_bq_main_events = ExternalTaskSensor(
        task_id="wait_for_bq_main_events",
        external_dag_id="copy_deduplicate",
        external_task_id="bq_main_events",
        execution_delta=datetime.timedelta(seconds=10800),
        check_existence=True,
        mode="reschedule",
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    telemetry_derived__adm_engagements_daily__v1.set_upstream(wait_for_bq_main_events)
    wait_for_event_events = ExternalTaskSensor(
        task_id="wait_for_event_events",
        external_dag_id="copy_deduplicate",
        external_task_id="event_events",
        execution_delta=datetime.timedelta(seconds=10800),
        check_existence=True,
        mode="reschedule",
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    telemetry_derived__adm_engagements_daily__v1.set_upstream(wait_for_event_events)
    wait_for_search_derived__search_clients_daily__v8 = ExternalTaskSensor(
        task_id="wait_for_search_derived__search_clients_daily__v8",
        external_dag_id="bqetl_search",
        external_task_id="search_derived__search_clients_daily__v8",
        execution_delta=datetime.timedelta(seconds=3600),
        check_existence=True,
        mode="reschedule",
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    telemetry_derived__adm_engagements_daily__v1.set_upstream(
        wait_for_search_derived__search_clients_daily__v8
    )
