# Generated via https://github.com/mozilla/bigquery-etl/blob/main/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskMarker
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
import datetime
from utils.constants import ALLOWED_STATES, FAILED_STATES
from utils.gcp import bigquery_etl_query, gke_command, bigquery_dq_check

docs = """
### bqetl_mobile_search

Built from bigquery-etl repo, [`dags/bqetl_mobile_search.py`](https://github.com/mozilla/bigquery-etl/blob/main/dags/bqetl_mobile_search.py)

#### Owner

anicholson@mozilla.com
"""


default_args = {
    "owner": "anicholson@mozilla.com",
    "start_date": datetime.datetime(2019, 7, 25, 0, 0),
    "end_date": None,
    "email": [
        "telemetry-alerts@mozilla.com",
        "anicholson@mozilla.com",
        "akomar@mozilla.com",
        "cmorales@mozilla.com",
    ],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=300),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
}

tags = ["impact/tier_1", "repo/bigquery-etl"]

with DAG(
    "bqetl_mobile_search",
    default_args=default_args,
    schedule_interval="0 2 * * *",
    doc_md=docs,
    tags=tags,
) as dag:
    search_derived__mobile_search_aggregates__v1 = bigquery_etl_query(
        task_id="search_derived__mobile_search_aggregates__v1",
        destination_table="mobile_search_aggregates_v1",
        dataset_id="search_derived",
        project_id="moz-fx-data-shared-prod",
        owner="akomar@mozilla.com",
        email=[
            "akomar@mozilla.com",
            "anicholson@mozilla.com",
            "cmorales@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    search_derived__mobile_search_clients_daily__v1 = bigquery_etl_query(
        task_id="search_derived__mobile_search_clients_daily__v1",
        destination_table="mobile_search_clients_daily_v1",
        dataset_id="search_derived",
        project_id="moz-fx-data-shared-prod",
        owner="akomar@mozilla.com",
        email=[
            "akomar@mozilla.com",
            "anicholson@mozilla.com",
            "cmorales@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    with TaskGroup(
        "search_derived__mobile_search_clients_daily__v1_external"
    ) as search_derived__mobile_search_clients_daily__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_analytics_aggregations__wait_for_search_derived__mobile_search_clients_daily__v1",
            external_dag_id="bqetl_analytics_aggregations",
            external_task_id="wait_for_search_derived__mobile_search_clients_daily__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=78300)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_kpis_shredder__wait_for_search_derived__mobile_search_clients_daily__v1",
            external_dag_id="bqetl_kpis_shredder",
            external_task_id="wait_for_search_derived__mobile_search_clients_daily__v1",
        )

        ExternalTaskMarker(
            task_id="bqetl_org_mozilla_firefox_derived__wait_for_search_derived__mobile_search_clients_daily__v1",
            external_dag_id="bqetl_org_mozilla_firefox_derived",
            external_task_id="wait_for_search_derived__mobile_search_clients_daily__v1",
        )

        ExternalTaskMarker(
            task_id="bqetl_mobile_activation__wait_for_search_derived__mobile_search_clients_daily__v1",
            external_dag_id="bqetl_mobile_activation",
            external_task_id="wait_for_search_derived__mobile_search_clients_daily__v1",
            execution_date="{{ (execution_date - macros.timedelta(seconds=7200)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_firefox_ios__wait_for_search_derived__mobile_search_clients_daily__v1",
            external_dag_id="bqetl_firefox_ios",
            external_task_id="wait_for_search_derived__mobile_search_clients_daily__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=79200)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_search_dashboard__wait_for_search_derived__mobile_search_clients_daily__v1",
            external_dag_id="bqetl_search_dashboard",
            external_task_id="wait_for_search_derived__mobile_search_clients_daily__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=77400)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_unified__wait_for_search_derived__mobile_search_clients_daily__v1",
            external_dag_id="bqetl_unified",
            external_task_id="wait_for_search_derived__mobile_search_clients_daily__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=82800)).isoformat() }}",
        )

        search_derived__mobile_search_clients_daily__v1_external.set_upstream(
            search_derived__mobile_search_clients_daily__v1
        )

    search_derived__mobile_search_clients_last_seen__v1 = bigquery_etl_query(
        task_id="search_derived__mobile_search_clients_last_seen__v1",
        destination_table="mobile_search_clients_last_seen_v1",
        dataset_id="search_derived",
        project_id="moz-fx-data-shared-prod",
        owner="akomar@mozilla.com",
        email=[
            "akomar@mozilla.com",
            "anicholson@mozilla.com",
            "cmorales@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=True,
    )

    search_derived__mobile_search_aggregates__v1.set_upstream(
        search_derived__mobile_search_clients_daily__v1
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

    search_derived__mobile_search_clients_daily__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    search_derived__mobile_search_clients_last_seen__v1.set_upstream(
        search_derived__mobile_search_clients_daily__v1
    )
