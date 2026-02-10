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
### bqetl_pageload_v1

Built from bigquery-etl repo, [`dags/bqetl_pageload_v1.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_pageload_v1.py)

#### Description

DAG to build pageload tables
#### Owner

wichan@mozilla.com

#### Tags

* impact/tier_1
* repo/bigquery-etl
"""


default_args = {
    "owner": "wichan@mozilla.com",
    "start_date": datetime.datetime(2024, 4, 1, 0, 0),
    "end_date": None,
    "email": ["telemetry-alerts@mozilla.com", "wichan@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
    "max_active_tis_per_dag": None,
}

tags = ["impact/tier_1", "repo/bigquery-etl"]

with DAG(
    "bqetl_pageload_v1",
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

    firefox_desktop_pageload_1pct__v1 = bigquery_etl_query(
        task_id="firefox_desktop_pageload_1pct__v1",
        destination_table="pageload_1pct_v1",
        dataset_id="firefox_desktop_derived",
        project_id="moz-fx-data-shared-prod",
        owner="wichan@mozilla.com",
        email=[
            "acreskey@mozilla.com",
            "dpalmeiro@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wichan@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        arguments=["--schema_update_option=ALLOW_FIELD_ADDITION"],
    )

    firefox_desktop_pageload_nightly__v1 = bigquery_etl_query(
        task_id="firefox_desktop_pageload_nightly__v1",
        destination_table="pageload_nightly_v1",
        dataset_id="firefox_desktop_derived",
        project_id="moz-fx-data-shared-prod",
        owner="wichan@mozilla.com",
        email=[
            "acreskey@mozilla.com",
            "dpalmeiro@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wichan@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        arguments=["--schema_update_option=ALLOW_FIELD_ADDITION"],
    )

    firefox_desktop_pageload_1pct__v1.set_upstream(wait_for_copy_deduplicate_all)

    firefox_desktop_pageload_nightly__v1.set_upstream(wait_for_copy_deduplicate_all)
