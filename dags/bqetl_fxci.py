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
### bqetl_fxci

Built from bigquery-etl repo, [`dags/bqetl_fxci.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_fxci.py)

#### Description

This DAG schedules queries for populating tables related to the
Firefox-CI Taskcluster instance.

#### Owner

ahalberstadt@mozilla.com

#### Tags

* impact/tier_3
* repo/bigquery-etl
"""


default_args = {
    "owner": "ahalberstadt@mozilla.com",
    "start_date": datetime.datetime(2020, 10, 11, 0, 0),
    "end_date": None,
    "email": ["ahalberstadt@mozilla.com", "telemetry-alerts@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
}

tags = ["impact/tier_3", "repo/bigquery-etl"]

with DAG(
    "bqetl_fxci",
    default_args=default_args,
    schedule_interval="0 18 * * *",
    doc_md=docs,
    tags=tags,
) as dag:

    fxci_derived__task_run_costs__v1 = bigquery_etl_query(
        task_id="fxci_derived__task_run_costs__v1",
        destination_table="task_run_costs_v1",
        dataset_id="fxci_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ahalberstadt@mozilla.com",
        email=["ahalberstadt@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    fxci_worker_cost__v1 = bigquery_etl_query(
        task_id="fxci_worker_cost__v1",
        destination_table="worker_costs_v1",
        dataset_id="fxci_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ahalberstadt@mozilla.com",
        email=["ahalberstadt@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    fxci_derived__task_run_costs__v1.set_upstream(fxci_worker_cost__v1)