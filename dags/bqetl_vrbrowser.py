# Generated via https://github.com/mozilla/bigquery-etl/blob/master/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from airflow.operators.sensors import ExternalTaskSensor
import datetime
from utils.gcp import bigquery_etl_query, gke_command

docs = """
### bqetl_vrbrowser

Built from bigquery-etl repo, [`dags/bqetl_vrbrowser.py`](https://github.com/mozilla/bigquery-etl/blob/master/dags/bqetl_vrbrowser.py)

#### Description

Custom ETL based on Glean pings from Mozilla VR Browser.
#### Owner

jklukas@mozilla.com
"""


default_args = {
    "owner": "jklukas@mozilla.com",
    "start_date": datetime.datetime(2019, 7, 25, 0, 0),
    "end_date": None,
    "email": [
        "telemetry-alerts@mozilla.com",
        "jklukas@mozilla.com",
        "ascholtz@mozilla.com",
    ],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=300),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
}

with DAG(
    "bqetl_vrbrowser",
    default_args=default_args,
    schedule_interval="0 2 * * *",
    doc_md=docs,
) as dag:

    org_mozilla_vrbrowser_derived__baseline_daily__v1 = bigquery_etl_query(
        task_id="org_mozilla_vrbrowser_derived__baseline_daily__v1",
        destination_table="baseline_daily_v1",
        dataset_id="org_mozilla_vrbrowser_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jklukas@mozilla.com",
        email=[
            "ascholtz@mozilla.com",
            "jklukas@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        dag=dag,
    )

    org_mozilla_vrbrowser_derived__clients_daily__v1 = bigquery_etl_query(
        task_id="org_mozilla_vrbrowser_derived__clients_daily__v1",
        destination_table="clients_daily_v1",
        dataset_id="org_mozilla_vrbrowser_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jklukas@mozilla.com",
        email=[
            "ascholtz@mozilla.com",
            "jklukas@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        dag=dag,
    )

    org_mozilla_vrbrowser_derived__clients_last_seen__v1 = bigquery_etl_query(
        task_id="org_mozilla_vrbrowser_derived__clients_last_seen__v1",
        destination_table="clients_last_seen_v1",
        dataset_id="org_mozilla_vrbrowser_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jklukas@mozilla.com",
        email=[
            "ascholtz@mozilla.com",
            "jklukas@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=True,
        dag=dag,
    )

    org_mozilla_vrbrowser_derived__metrics_daily__v1 = bigquery_etl_query(
        task_id="org_mozilla_vrbrowser_derived__metrics_daily__v1",
        destination_table="metrics_daily_v1",
        dataset_id="org_mozilla_vrbrowser_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jklukas@mozilla.com",
        email=[
            "ascholtz@mozilla.com",
            "jklukas@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
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

    org_mozilla_vrbrowser_derived__baseline_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    org_mozilla_vrbrowser_derived__clients_daily__v1.set_upstream(
        org_mozilla_vrbrowser_derived__baseline_daily__v1
    )

    org_mozilla_vrbrowser_derived__clients_daily__v1.set_upstream(
        org_mozilla_vrbrowser_derived__metrics_daily__v1
    )

    org_mozilla_vrbrowser_derived__clients_last_seen__v1.set_upstream(
        org_mozilla_vrbrowser_derived__clients_daily__v1
    )

    org_mozilla_vrbrowser_derived__metrics_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )
