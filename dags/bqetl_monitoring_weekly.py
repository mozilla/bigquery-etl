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
### bqetl_monitoring_weekly

Built from bigquery-etl repo, [`dags/bqetl_monitoring_weekly.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_monitoring_weekly.py)

#### Description

This DAG populates monitoring datasets that only need to be updated weekly

#### Owner

kwindau@mozilla.com

#### Tags

* impact/tier_3
* repo/bigquery-etl
"""


default_args = {
    "owner": "kwindau@mozilla.com",
    "start_date": datetime.datetime(2024, 10, 25, 0, 0),
    "end_date": None,
    "email": ["kwindau@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
    "max_active_tis_per_dag": None,
}

tags = ["impact/tier_3", "repo/bigquery-etl"]

with DAG(
    "bqetl_monitoring_weekly",
    default_args=default_args,
    schedule_interval="40 12 * * 7",
    doc_md=docs,
    tags=tags,
    catchup=False,
) as dag:

    monitoring_derived__metadata_completeness__v1 = bigquery_etl_query(
        task_id="monitoring_derived__metadata_completeness__v1",
        destination_table="metadata_completeness_v1",
        dataset_id="monitoring_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    monitoring_derived__metadata_standardization__v1 = bigquery_etl_query(
        task_id="monitoring_derived__metadata_standardization__v1",
        destination_table="metadata_standardization_v1",
        dataset_id="monitoring_derived",
        project_id="moz-fx-data-shared-prod",
        owner="lvargas@mozilla.com",
        email=["kwindau@mozilla.com", "lvargas@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )
