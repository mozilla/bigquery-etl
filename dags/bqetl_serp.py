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
### bqetl_serp

Built from bigquery-etl repo, [`dags/bqetl_serp.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_serp.py)

#### Description

DAG to build serp events data
#### Owner

akommasani@mozilla.com

#### Tags

* impact/tier_1
* repo/bigquery-etl
"""


default_args = {
    "owner": "akommasani@mozilla.com",
    "start_date": datetime.datetime(2023, 10, 1, 0, 0),
    "end_date": None,
    "email": ["telemetry-alerts@mozilla.com", "akommasani@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
    "max_active_tis_per_dag": None,
}

tags = ["impact/tier_1", "repo/bigquery-etl"]

with DAG(
    "bqetl_serp",
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

    firefox_desktop_serp_events__v2 = bigquery_etl_query(
        task_id="firefox_desktop_serp_events__v2",
        destination_table='serp_events_v2${{ (execution_date - macros.timedelta(hours=24)).strftime("%Y%m%d") }}',
        dataset_id="firefox_desktop_derived",
        project_id="moz-fx-data-shared-prod",
        owner="akommasani@mozilla.com",
        email=[
            "akommasani@mozilla.com",
            "betling@mozilla.com",
            "kbammarito@mozilla.com",
            "pissac@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        parameters=["submission_date:DATE:{{ds}}"],
        sql_file_path="sql/moz-fx-data-shared-prod/firefox_desktop_derived/serp_events_v2/query.sql",
    )

    with TaskGroup(
        "firefox_desktop_serp_events__v2_external",
    ) as firefox_desktop_serp_events__v2_external:
        ExternalTaskMarker(
            task_id="bqetl_search_dashboard__wait_for_firefox_desktop_serp_events__v2",
            external_dag_id="bqetl_search_dashboard",
            external_task_id="wait_for_firefox_desktop_serp_events__v2",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=66600)).isoformat() }}",
        )

        firefox_desktop_serp_events__v2_external.set_upstream(
            firefox_desktop_serp_events__v2
        )

    firefox_desktop_serp_events__v2.set_upstream(wait_for_copy_deduplicate_all)
