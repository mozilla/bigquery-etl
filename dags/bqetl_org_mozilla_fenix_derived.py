# Generated via https://github.com/mozilla/bigquery-etl/blob/master/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from airflow.operators.sensors import ExternalTaskSensor
import datetime
from utils.gcp import bigquery_etl_query, gke_command

docs = """
### bqetl_org_mozilla_fenix_derived

Built from bigquery-etl repo, [`dags/bqetl_org_mozilla_fenix_derived.py`](https://github.com/mozilla/bigquery-etl/blob/master/dags/bqetl_org_mozilla_fenix_derived.py)

#### Owner

amiyaguchi@mozilla.com
"""


default_args = {
    "owner": "amiyaguchi@mozilla.com",
    "start_date": datetime.datetime(2020, 10, 18, 0, 0),
    "end_date": None,
    "email": ["amiyaguchi@mozilla.com", "telemetry-alerts@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
}

with DAG(
    "bqetl_org_mozilla_fenix_derived",
    default_args=default_args,
    schedule_interval="@daily",
    doc_md=docs,
) as dag:

    org_mozilla_fenix_derived__geckoview_version__v1 = bigquery_etl_query(
        task_id="org_mozilla_fenix_derived__geckoview_version__v1",
        destination_table="geckoview_version_v1",
        dataset_id="org_mozilla_fenix_derived",
        project_id="moz-fx-data-shared-prod",
        owner="amiyaguchi@mozilla.com",
        email=["amiyaguchi@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        dag=dag,
    )

    wait_for_copy_deduplicate_all = ExternalTaskSensor(
        task_id="wait_for_copy_deduplicate_all",
        external_dag_id="copy_deduplicate",
        external_task_id="copy_deduplicate_all",
        execution_delta=datetime.timedelta(days=-1, seconds=82800),
        check_existence=True,
        mode="reschedule",
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    org_mozilla_fenix_derived__geckoview_version__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )
