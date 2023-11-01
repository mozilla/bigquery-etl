# Generated via https://github.com/mozilla/bigquery-etl/blob/main/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskMarker
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
import datetime
from utils.constants import ALLOWED_STATES, FAILED_STATES
from utils.gcp import bigquery_etl_query, gke_command, bigquery_dq_check

docs = """
### bqetl_search

Built from bigquery-etl repo, [`dags/bqetl_search.py`](https://github.com/mozilla/bigquery-etl/blob/main/dags/bqetl_search.py)

#### Owner

anicholson@mozilla.com
"""


default_args = {
    "owner": "anicholson@mozilla.com",
    "start_date": datetime.datetime(2018, 11, 27, 0, 0),
    "end_date": None,
    "email": [
        "telemetry-alerts@mozilla.com",
        "anicholson@mozilla.com",
        "akomar@mozilla.com",
        "cmorales@mozilla.com",
    ],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
}

tags = ["impact/tier_1", "repo/bigquery-etl"]

with DAG(
    "bqetl_search",
    default_args=default_args,
    schedule_interval="0 3 * * *",
    doc_md=docs,
    tags=tags,
) as dag:
    search_derived__search_aggregates__v8 = bigquery_etl_query(
        task_id="search_derived__search_aggregates__v8",
        destination_table="search_aggregates_v8",
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
        "search_derived__search_aggregates__v8_external"
    ) as search_derived__search_aggregates__v8_external:
        ExternalTaskMarker(
            task_id="bqetl_search_dashboard__wait_for_search_derived__search_aggregates__v8",
            external_dag_id="bqetl_search_dashboard",
            external_task_id="wait_for_search_derived__search_aggregates__v8",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=82800)).isoformat() }}",
        )

        search_derived__search_aggregates__v8_external.set_upstream(
            search_derived__search_aggregates__v8
        )

    search_derived__search_clients_daily__v8 = bigquery_etl_query(
        task_id="search_derived__search_clients_daily__v8",
        destination_table="search_clients_daily_v8",
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
        "search_derived__search_clients_daily__v8_external"
    ) as search_derived__search_clients_daily__v8_external:
        ExternalTaskMarker(
            task_id="bqetl_ctxsvc_derived__wait_for_search_derived__search_clients_daily__v8",
            external_dag_id="bqetl_ctxsvc_derived",
            external_task_id="wait_for_search_derived__search_clients_daily__v8",
        )

        ExternalTaskMarker(
            task_id="bqetl_review_checker__wait_for_search_derived__search_clients_daily__v8",
            external_dag_id="bqetl_review_checker",
            external_task_id="wait_for_search_derived__search_clients_daily__v8",
            execution_date="{{ (execution_date - macros.timedelta(seconds=10800)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_search_dashboard__wait_for_search_derived__search_clients_daily__v8",
            external_dag_id="bqetl_search_dashboard",
            external_task_id="wait_for_search_derived__search_clients_daily__v8",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=82800)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_addons__wait_for_search_derived__search_clients_daily__v8",
            external_dag_id="bqetl_addons",
            external_task_id="wait_for_search_derived__search_clients_daily__v8",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=82800)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="jetstream__wait_for_search_clients_daily",
            external_dag_id="jetstream",
            external_task_id="wait_for_search_clients_daily",
            execution_date="{{ (execution_date + macros.timedelta(seconds=3600)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="operational_monitoring__wait_for_search_clients_daily",
            external_dag_id="operational_monitoring",
            external_task_id="wait_for_search_clients_daily",
            execution_date="{{ (execution_date + macros.timedelta(seconds=3600)).isoformat() }}",
        )

        search_derived__search_clients_daily__v8_external.set_upstream(
            search_derived__search_clients_daily__v8
        )

    search_derived__search_clients_last_seen__v1 = bigquery_etl_query(
        task_id="search_derived__search_clients_last_seen__v1",
        destination_table="search_clients_last_seen_v1",
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

    with TaskGroup(
        "search_derived__search_clients_last_seen__v1_external"
    ) as search_derived__search_clients_last_seen__v1_external:
        ExternalTaskMarker(
            task_id="ltv_daily__wait_for_search_clients_last_seen",
            external_dag_id="ltv_daily",
            external_task_id="wait_for_search_clients_last_seen",
            execution_date="{{ (execution_date + macros.timedelta(seconds=3600)).isoformat() }}",
        )

        search_derived__search_clients_last_seen__v1_external.set_upstream(
            search_derived__search_clients_last_seen__v1
        )

    search_derived__search_metric_contribution__v1 = bigquery_etl_query(
        task_id="search_derived__search_metric_contribution__v1",
        destination_table="search_metric_contribution_v1",
        dataset_id="search_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jklukas@mozilla.com",
        email=[
            "akomar@mozilla.com",
            "anicholson@mozilla.com",
            "cmorales@mozilla.com",
            "jklukas@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    search_derived__search_aggregates__v8.set_upstream(
        search_derived__search_clients_daily__v8
    )

    wait_for_telemetry_derived__clients_daily_joined__v1 = ExternalTaskSensor(
        task_id="wait_for_telemetry_derived__clients_daily_joined__v1",
        external_dag_id="bqetl_main_summary",
        external_task_id="telemetry_derived__clients_daily_joined__v1",
        execution_delta=datetime.timedelta(seconds=3600),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    search_derived__search_clients_daily__v8.set_upstream(
        wait_for_telemetry_derived__clients_daily_joined__v1
    )

    search_derived__search_clients_last_seen__v1.set_upstream(
        search_derived__search_clients_daily__v8
    )

    search_derived__search_metric_contribution__v1.set_upstream(
        search_derived__search_clients_daily__v8
    )
