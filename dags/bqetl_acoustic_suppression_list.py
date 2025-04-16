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
### bqetl_acoustic_suppression_list

Built from bigquery-etl repo, [`dags/bqetl_acoustic_suppression_list.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_acoustic_suppression_list.py)

#### Description

ETL for Acoustic suppression list.

#### Owner

cbeck@mozilla.com

#### Tags

* impact/tier_3
* repo/bigquery-etl
"""


default_args = {
    "owner": "cbeck@mozilla.com",
    "start_date": datetime.datetime(2024, 4, 3, 0, 0),
    "end_date": None,
    "email": ["cbeck@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
    "max_active_tis_per_dag": None,
}

tags = ["impact/tier_3", "repo/bigquery-etl"]

with DAG(
    "bqetl_acoustic_suppression_list",
    default_args=default_args,
    schedule_interval="0 9 * * *",
    doc_md=docs,
    tags=tags,
    catchup=False,
) as dag:

    acoustic_external__suppression_list__v1 = bigquery_etl_query(
        task_id="acoustic_external__suppression_list__v1",
        destination_table="suppression_list_v1",
        dataset_id="acoustic_external",
        project_id="moz-fx-data-shared-prod",
        owner="cbeck@mozilla.com",
        email=["cbeck@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    with TaskGroup(
        "acoustic_external__suppression_list__v1_external",
    ) as acoustic_external__suppression_list__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_marketing_suppression_list__wait_for_acoustic_external__suppression_list__v1",
            external_dag_id="bqetl_marketing_suppression_list",
            external_task_id="wait_for_acoustic_external__suppression_list__v1",
            execution_date="{{ (execution_date - macros.timedelta(seconds=21600)).isoformat() }}",
        )

        acoustic_external__suppression_list__v1_external.set_upstream(
            acoustic_external__suppression_list__v1
        )
