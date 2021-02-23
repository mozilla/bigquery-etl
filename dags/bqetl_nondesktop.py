# Generated via https://github.com/mozilla/bigquery-etl/blob/master/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from airflow.operators.sensors import ExternalTaskSensor
import datetime
from utils.gcp import bigquery_etl_query, gke_command

docs = """
### bqetl_nondesktop

Built from bigquery-etl repo, [`dags/bqetl_nondesktop.py`](https://github.com/mozilla/bigquery-etl/blob/master/dags/bqetl_nondesktop.py)

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
    "bqetl_nondesktop",
    default_args=default_args,
    schedule_interval="0 3 * * *",
    doc_md=docs,
) as dag:

    firefox_nondesktop_exact_mau28_by_client_count_dimensions = bigquery_etl_query(
        task_id="firefox_nondesktop_exact_mau28_by_client_count_dimensions",
        destination_table="firefox_nondesktop_exact_mau28_by_client_count_dimensions_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jklukas@mozilla.com",
        email=["jklukas@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        dag=dag,
    )

    telemetry_derived__firefox_nondesktop_day_2_7_activation__v1 = bigquery_etl_query(
        task_id="telemetry_derived__firefox_nondesktop_day_2_7_activation__v1",
        destination_table="firefox_nondesktop_day_2_7_activation_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="gkaberere@mozilla.com",
        email=["gkaberere@mozilla.com", "jklukas@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        dag=dag,
    )

    telemetry_derived__firefox_nondesktop_exact_mau28__v1 = bigquery_etl_query(
        task_id="telemetry_derived__firefox_nondesktop_exact_mau28__v1",
        destination_table="firefox_nondesktop_exact_mau28_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jklukas@mozilla.com",
        email=["jklukas@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        dag=dag,
    )

    telemetry_derived__mobile_usage__v1 = bigquery_etl_query(
        task_id="telemetry_derived__mobile_usage__v1",
        destination_table="mobile_usage_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jklukas@mozilla.com",
        email=["jklukas@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        dag=dag,
    )

    wait_for_telemetry_derived__core_clients_last_seen__v1 = ExternalTaskSensor(
        task_id="wait_for_telemetry_derived__core_clients_last_seen__v1",
        external_dag_id="bqetl_core",
        external_task_id="telemetry_derived__core_clients_last_seen__v1",
        execution_delta=datetime.timedelta(seconds=3600),
        check_existence=True,
        mode="reschedule",
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    firefox_nondesktop_exact_mau28_by_client_count_dimensions.set_upstream(
        wait_for_telemetry_derived__core_clients_last_seen__v1
    )

    wait_for_baseline_clients_last_seen = ExternalTaskSensor(
        task_id="wait_for_baseline_clients_last_seen",
        external_dag_id="copy_deduplicate",
        external_task_id="baseline_clients_last_seen",
        execution_delta=datetime.timedelta(seconds=7200),
        check_existence=True,
        mode="reschedule",
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    telemetry_derived__firefox_nondesktop_day_2_7_activation__v1.set_upstream(
        wait_for_baseline_clients_last_seen
    )
    telemetry_derived__firefox_nondesktop_day_2_7_activation__v1.set_upstream(
        wait_for_telemetry_derived__core_clients_last_seen__v1
    )

    telemetry_derived__firefox_nondesktop_exact_mau28__v1.set_upstream(
        wait_for_baseline_clients_last_seen
    )
    telemetry_derived__firefox_nondesktop_exact_mau28__v1.set_upstream(
        wait_for_telemetry_derived__core_clients_last_seen__v1
    )

    telemetry_derived__mobile_usage__v1.set_upstream(
        telemetry_derived__firefox_nondesktop_exact_mau28__v1
    )
