# Generated via https://github.com/mozilla/bigquery-etl/blob/master/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from airflow.operators.sensors import ExternalTaskSensor
import datetime
from utils.gcp import bigquery_etl_query, gke_command

docs = """
### bqetl_asn_aggregates

Built from bigquery-etl repo, [`dags/bqetl_asn_aggregates.py`](https://github.com/mozilla/bigquery-etl/blob/master/dags/bqetl_asn_aggregates.py)

#### Description

The DAG schedules ASN aggregates queries.
#### Owner

ascholtz@mozilla.com
"""


default_args = {
    "owner": "ascholtz@mozilla.com",
    "start_date": datetime.datetime(2020, 4, 5, 0, 0),
    "end_date": None,
    "email": ["ascholtz@mozilla.com", "tdsmith@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
}

with DAG(
    "bqetl_asn_aggregates",
    default_args=default_args,
    schedule_interval="0 2 * * *",
    doc_md=docs,
) as dag:

    telemetry_derived__asn_aggregates__v1 = bigquery_etl_query(
        task_id="telemetry_derived__asn_aggregates__v1",
        destination_table="asn_aggregates_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="tdsmith@mozilla.com",
        email=["ascholtz@mozilla.com", "tdsmith@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        parameters=["n_clients:INT64:500"],
        dag=dag,
    )

    wait_for_bq_main_events = ExternalTaskSensor(
        task_id="wait_for_bq_main_events",
        external_dag_id="copy_deduplicate",
        external_task_id="bq_main_events",
        execution_delta=datetime.timedelta(seconds=3600),
        check_existence=True,
        mode="reschedule",
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    telemetry_derived__asn_aggregates__v1.set_upstream(wait_for_bq_main_events)
