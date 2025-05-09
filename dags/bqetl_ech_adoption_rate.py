# Generated via https://github.com/mozilla/bigquery-etl/blob/main/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskMarker
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
import datetime
from operators.gcp_container_operator import GKEPodOperator
from utils.constants import ALLOWED_STATES, FAILED_STATES
from utils.gcp import bigquery_etl_query, bigquery_dq_check, bigquery_bigeye_check

docs = """
### bqetl_ech_adoption_rate

Built from bigquery-etl repo, [`dags/bqetl_ech_adoption_rate.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_ech_adoption_rate.py)

#### Description

Tracks ECH adoption across countries and ISPs over time.
To be disabled after 2025-10-31.
bug 1957082

#### Owner

efilho@mozilla.com

#### Tags

* impact/tier_2
* repo/bigquery-etl
"""


default_args = {
    "owner": "efilho@mozilla.com",
    "start_date": datetime.datetime(2025, 4, 28, 0, 0),
    "end_date": datetime.datetime(2025, 10, 31, 0, 0),
    "email": ["telemetry-alerts@mozilla.com", "efilho@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
    "max_active_tis_per_dag": None,
}

tags = ["impact/tier_2", "repo/bigquery-etl"]

with DAG(
    "bqetl_ech_adoption_rate",
    default_args=default_args,
    schedule_interval="@daily",
    doc_md=docs,
    tags=tags,
    catchup=False,
) as dag:

    wait_for_copy_deduplicate_all = ExternalTaskSensor(
        task_id="wait_for_copy_deduplicate_all",
        external_dag_id="copy_deduplicate",
        external_task_id="copy_deduplicate_all",
        execution_delta=datetime.timedelta(days=-1, seconds=82800),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    telemetry_derived__ech_adoption_rate__v1 = bigquery_etl_query(
        task_id="telemetry_derived__ech_adoption_rate__v1",
        destination_table="ech_adoption_rate_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="efilho@mozilla.com",
        email=["efilho@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    telemetry_derived__ech_adoption_rate__v1.set_upstream(wait_for_copy_deduplicate_all)
