# Generated via https://github.com/mozilla/bigquery-etl/blob/main/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskMarker
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
import datetime
from utils.constants import ALLOWED_STATES, FAILED_STATES
from utils.gcp import bigquery_etl_query, gke_command

docs = """
### bqetl_org_mozilla_focus_derived

Built from bigquery-etl repo, [`dags/bqetl_org_mozilla_focus_derived.py`](https://github.com/mozilla/bigquery-etl/blob/main/dags/bqetl_org_mozilla_focus_derived.py)

#### Owner

dthorn@mozilla.com
"""


default_args = {
    "owner": "dthorn@mozilla.com",
    "start_date": datetime.datetime(2023, 2, 22, 0, 0),
    "end_date": None,
    "email": ["dthorn@mozilla.com", "telemetry-alerts@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
}

tags = ["impact/tier_1", "repo/bigquery-etl"]

with DAG(
    "bqetl_org_mozilla_focus_derived",
    default_args=default_args,
    schedule_interval="0 2 * * *",
    doc_md=docs,
    tags=tags,
) as dag:
    org_mozilla_focus_beta_derived__additional_deletion_requests__v1 = (
        bigquery_etl_query(
            task_id="org_mozilla_focus_beta_derived__additional_deletion_requests__v1",
            destination_table="additional_deletion_requests_v1",
            dataset_id="org_mozilla_focus_beta_derived",
            project_id="moz-fx-data-shared-prod",
            owner="dthorn@mozilla.com",
            email=["dthorn@mozilla.com", "telemetry-alerts@mozilla.com"],
            date_partition_parameter="submission_date",
            depends_on_past=False,
        )
    )

    org_mozilla_focus_derived__additional_deletion_requests__v1 = bigquery_etl_query(
        task_id="org_mozilla_focus_derived__additional_deletion_requests__v1",
        destination_table="additional_deletion_requests_v1",
        dataset_id="org_mozilla_focus_derived",
        project_id="moz-fx-data-shared-prod",
        owner="dthorn@mozilla.com",
        email=["dthorn@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    org_mozilla_focus_nightly_derived__additional_deletion_requests__v1 = bigquery_etl_query(
        task_id="org_mozilla_focus_nightly_derived__additional_deletion_requests__v1",
        destination_table="additional_deletion_requests_v1",
        dataset_id="org_mozilla_focus_nightly_derived",
        project_id="moz-fx-data-shared-prod",
        owner="dthorn@mozilla.com",
        email=["dthorn@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    wait_for_copy_deduplicate_all = ExternalTaskSensor(
        task_id="wait_for_copy_deduplicate_all",
        external_dag_id="copy_deduplicate",
        external_task_id="copy_deduplicate_all",
        execution_delta=datetime.timedelta(seconds=3600),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    org_mozilla_focus_beta_derived__additional_deletion_requests__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    org_mozilla_focus_derived__additional_deletion_requests__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    org_mozilla_focus_nightly_derived__additional_deletion_requests__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )
