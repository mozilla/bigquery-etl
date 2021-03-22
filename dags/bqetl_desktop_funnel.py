# Generated via https://github.com/mozilla/bigquery-etl/blob/master/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from airflow.operators.sensors import ExternalTaskSensor
import datetime
from utils.gcp import bigquery_etl_query, gke_command

docs = """
### bqetl_desktop_funnel

Built from bigquery-etl repo, [`dags/bqetl_desktop_funnel.py`](https://github.com/mozilla/bigquery-etl/blob/master/dags/bqetl_desktop_funnel.py)

#### Description

This DAG schedules desktop funnel queries used to power the [Numbers that Matter dashboard](https://protosaur.dev/numbers-that-matter/)

#### Owner

ascholtz@mozilla.com
"""


default_args = {
    "owner": "ascholtz@mozilla.com",
    "start_date": datetime.datetime(2021, 1, 1, 0, 0),
    "end_date": None,
    "email": ["telemetry-alerts@mozilla.com", "ascholtz@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
}

with DAG(
    "bqetl_desktop_funnel",
    default_args=default_args,
    schedule_interval="0 4 * * *",
    doc_md=docs,
) as dag:

    telemetry_derived__desktop_funnel_activation_day_6__v1 = bigquery_etl_query(
        task_id="telemetry_derived__desktop_funnel_activation_day_6__v1",
        destination_table="desktop_funnel_activation_day_6_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        dag=dag,
    )

    telemetry_derived__desktop_funnel_installs__v1 = bigquery_etl_query(
        task_id="telemetry_derived__desktop_funnel_installs__v1",
        destination_table="desktop_funnel_installs_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        dag=dag,
    )

    telemetry_derived__desktop_funnel_new_profiles__v1 = bigquery_etl_query(
        task_id="telemetry_derived__desktop_funnel_new_profiles__v1",
        destination_table="desktop_funnel_new_profiles_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        dag=dag,
    )

    wait_for_copy_deduplicate_all = ExternalTaskSensor(
        task_id="wait_for_copy_deduplicate_all",
        external_dag_id="copy_deduplicate",
        external_task_id="copy_deduplicate_all",
        execution_delta=datetime.timedelta(seconds=10800),
        check_existence=True,
        mode="reschedule",
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    telemetry_derived__desktop_funnel_activation_day_6__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )
    wait_for_telemetry_derived__clients_last_seen__v1 = ExternalTaskSensor(
        task_id="wait_for_telemetry_derived__clients_last_seen__v1",
        external_dag_id="bqetl_main_summary",
        external_task_id="telemetry_derived__clients_last_seen__v1",
        execution_delta=datetime.timedelta(seconds=7200),
        check_existence=True,
        mode="reschedule",
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    telemetry_derived__desktop_funnel_activation_day_6__v1.set_upstream(
        wait_for_telemetry_derived__clients_last_seen__v1
    )

    telemetry_derived__desktop_funnel_installs__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    telemetry_derived__desktop_funnel_new_profiles__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )
