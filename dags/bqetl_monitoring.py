# Generated via https://github.com/mozilla/bigquery-etl/blob/main/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskMarker
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
import datetime
from utils.constants import ALLOWED_STATES, FAILED_STATES
from utils.gcp import bigquery_etl_query, gke_command, bigquery_dq_check

docs = """
### bqetl_monitoring

Built from bigquery-etl repo, [`dags/bqetl_monitoring.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_monitoring.py)

#### Description

This DAG schedules queries and scripts for populating datasets
used for monitoring of the data platform.

#### Owner

ascholtz@mozilla.com

#### Tags

* impact/tier_1
* repo/bigquery-etl
"""


default_args = {
    "owner": "ascholtz@mozilla.com",
    "start_date": datetime.datetime(2018, 10, 30, 0, 0),
    "end_date": None,
    "email": ["ascholtz@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
}

tags = ["impact/tier_1", "repo/bigquery-etl"]

with DAG(
    "bqetl_monitoring",
    default_args=default_args,
    schedule_interval="0 2 * * *",
    doc_md=docs,
    tags=tags,
) as dag:
    glean_server_knob_experiments__v1 = bigquery_etl_query(
        task_id="glean_server_knob_experiments__v1",
        destination_table="glean_server_knob_experiments_v1",
        dataset_id="monitoring_derived",
        project_id="moz-fx-data-shared-prod",
        owner="wstuckey@mozilla.com",
        email=["ascholtz@mozilla.com", "wstuckey@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    monitoring_derived__average_ping_sizes__v1 = gke_command(
        task_id="monitoring_derived__average_ping_sizes__v1",
        command=[
            "python",
            "sql/moz-fx-data-shared-prod/monitoring_derived/average_ping_sizes_v1/query.py",
        ]
        + ["--date", "{{ ds }}"],
        docker_image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com"],
    )

    monitoring_derived__bigquery_etl_scheduled_queries_cost__v1 = gke_command(
        task_id="monitoring_derived__bigquery_etl_scheduled_queries_cost__v1",
        command=[
            "python",
            "sql/moz-fx-data-shared-prod/monitoring_derived/bigquery_etl_scheduled_queries_cost_v1/query.py",
        ]
        + ["--date", "{{ ds }}"],
        docker_image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com"],
    )

    monitoring_derived__bigquery_etl_scheduled_query_usage__v1 = gke_command(
        task_id="monitoring_derived__bigquery_etl_scheduled_query_usage__v1",
        command=[
            "python",
            "sql/moz-fx-data-shared-prod/monitoring_derived/bigquery_etl_scheduled_query_usage_v1/query.py",
        ]
        + ["--date", "{{ ds }}"],
        docker_image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com"],
    )

    monitoring_derived__bigquery_table_storage__v1 = gke_command(
        task_id="monitoring_derived__bigquery_table_storage__v1",
        command=[
            "python",
            "sql/moz-fx-data-shared-prod/monitoring_derived/bigquery_table_storage_v1/query.py",
        ]
        + [],
        docker_image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
        owner="wichan@mozilla.com",
        email=["ascholtz@mozilla.com", "wichan@mozilla.com"],
    )

    monitoring_derived__bigquery_table_storage_timeline_daily__v1 = gke_command(
        task_id="monitoring_derived__bigquery_table_storage_timeline_daily__v1",
        command=[
            "python",
            "sql/moz-fx-data-shared-prod/monitoring_derived/bigquery_table_storage_timeline_daily_v1/query.py",
        ]
        + ["--date", "{{ ds }}"],
        docker_image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
        owner="wichan@mozilla.com",
        email=["ascholtz@mozilla.com", "wichan@mozilla.com"],
    )

    monitoring_derived__bigquery_tables_inventory__v1 = gke_command(
        task_id="monitoring_derived__bigquery_tables_inventory__v1",
        command=[
            "python",
            "sql/moz-fx-data-shared-prod/monitoring_derived/bigquery_tables_inventory_v1/query.py",
        ]
        + ["--date", "{{ ds }}"],
        docker_image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
        owner="wichan@mozilla.com",
        email=["ascholtz@mozilla.com", "wichan@mozilla.com"],
    )

    monitoring_derived__bigquery_usage__v1 = gke_command(
        task_id="monitoring_derived__bigquery_usage__v1",
        command=[
            "python",
            "sql/moz-fx-data-shared-prod/monitoring_derived/bigquery_usage_v1/query.py",
        ]
        + ["--date", "{{ ds }}"],
        docker_image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
        owner="wichan@mozilla.com",
        email=["ascholtz@mozilla.com", "wichan@mozilla.com"],
    )

    monitoring_derived__bigquery_usage__v2 = bigquery_etl_query(
        task_id="monitoring_derived__bigquery_usage__v2",
        destination_table="bigquery_usage_v2",
        dataset_id="monitoring_derived",
        project_id="moz-fx-data-shared-prod",
        owner="wichan@mozilla.com",
        email=["ascholtz@mozilla.com", "mhirose@mozilla.com", "wichan@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    monitoring_derived__column_size__v1 = gke_command(
        task_id="monitoring_derived__column_size__v1",
        command=[
            "python",
            "sql/moz-fx-data-shared-prod/monitoring_derived/column_size_v1/query.py",
        ]
        + ["--date", "{{ ds }}"],
        docker_image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com"],
    )

    monitoring_derived__event_error_monitoring_aggregates__v1 = bigquery_etl_query(
        task_id="monitoring_derived__event_error_monitoring_aggregates__v1",
        destination_table="event_error_monitoring_aggregates_v1",
        dataset_id="monitoring_derived",
        project_id="moz-fx-data-shared-prod",
        owner="akomar@mozilla.com",
        email=["akomar@mozilla.com", "ascholtz@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    monitoring_derived__event_flow_monitoring_aggregates__v1 = bigquery_etl_query(
        task_id="monitoring_derived__event_flow_monitoring_aggregates__v1",
        destination_table=None,
        dataset_id="monitoring_derived",
        project_id="moz-fx-data-shared-prod",
        owner="akomar@mozilla.com",
        email=["akomar@mozilla.com", "ascholtz@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        parameters=["submission_date:DATE:{{ds}}"],
        sql_file_path="sql/moz-fx-data-shared-prod/monitoring_derived/event_flow_monitoring_aggregates_v1/script.sql",
    )

    monitoring_derived__event_monitoring_aggregates__v1 = bigquery_etl_query(
        task_id="monitoring_derived__event_monitoring_aggregates__v1",
        destination_table="event_monitoring_aggregates_v1",
        dataset_id="monitoring_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["akomar@mozilla.com", "ascholtz@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    monitoring_derived__jobs_by_organization__v1 = gke_command(
        task_id="monitoring_derived__jobs_by_organization__v1",
        command=[
            "python",
            "sql/moz-fx-data-shared-prod/monitoring_derived/jobs_by_organization_v1/query.py",
        ]
        + ["--date", "{{ ds }}"],
        docker_image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
        owner="mhirose@mozilla.com",
        email=["ascholtz@mozilla.com", "mhirose@mozilla.com"],
    )

    monitoring_derived__schema_error_counts__v2 = bigquery_etl_query(
        task_id="monitoring_derived__schema_error_counts__v2",
        destination_table="schema_error_counts_v2",
        dataset_id="monitoring_derived",
        project_id="moz-fx-data-shared-prod",
        owner="amiyaguchi@mozilla.com",
        email=["amiyaguchi@mozilla.com", "ascholtz@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    monitoring_derived__shredder_rows_deleted__v1 = gke_command(
        task_id="monitoring_derived__shredder_rows_deleted__v1",
        command=[
            "python",
            "sql/moz-fx-data-shared-prod/monitoring_derived/shredder_rows_deleted_v1/query.py",
        ]
        + [
            "--end_date",
            "{{ds}}",
            "--destination_table",
            "moz-fx-data-shared-prod.monitoring_derived.shredder_rows_deleted_v1${{ds_nodash}}",
        ],
        docker_image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
        owner="akomar@mozilla.com",
        email=["akomar@mozilla.com", "ascholtz@mozilla.com"],
    )

    monitoring_derived__stable_and_derived_table_sizes__v1 = gke_command(
        task_id="monitoring_derived__stable_and_derived_table_sizes__v1",
        command=[
            "python",
            "sql/moz-fx-data-shared-prod/monitoring_derived/stable_and_derived_table_sizes_v1/query.py",
        ]
        + ["--date", "{{ macros.ds_add(ds, -1) }}"],
        docker_image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com"],
    )

    monitoring_derived__stable_table_column_counts__v1 = bigquery_etl_query(
        task_id="monitoring_derived__stable_table_column_counts__v1",
        destination_table=None,
        dataset_id="monitoring_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        parameters=["submission_date:DATE:{{ds}}"],
        sql_file_path="sql/moz-fx-data-shared-prod/monitoring_derived/stable_table_column_counts_v1/script.sql",
    )

    monitoring_derived__structured_error_counts__v2 = bigquery_etl_query(
        task_id="monitoring_derived__structured_error_counts__v2",
        destination_table=None,
        dataset_id="monitoring_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        parameters=["submission_date:DATE:{{ds}}"],
        sql_file_path="sql/moz-fx-data-shared-prod/monitoring_derived/structured_error_counts_v2/script.sql",
    )

    monitoring_derived__structured_missing_columns__v1 = gke_command(
        task_id="monitoring_derived__structured_missing_columns__v1",
        command=[
            "python",
            "sql/moz-fx-data-shared-prod/monitoring_derived/structured_missing_columns_v1/query.py",
        ]
        + ["--date", "{{ ds }}"],
        docker_image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
        owner="amiyaguchi@mozilla.com",
        email=["amiyaguchi@mozilla.com", "ascholtz@mozilla.com"],
    )

    monitoring_derived__telemetry_missing_columns__v3 = bigquery_etl_query(
        task_id="monitoring_derived__telemetry_missing_columns__v3",
        destination_table="telemetry_missing_columns_v3",
        dataset_id="monitoring_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    wait_for_monitoring__experimenter_experiments__v1 = ExternalTaskSensor(
        task_id="wait_for_monitoring__experimenter_experiments__v1",
        external_dag_id="bqetl_experimenter_experiments_import",
        external_task_id="monitoring__experimenter_experiments__v1",
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    glean_server_knob_experiments__v1.set_upstream(
        wait_for_monitoring__experimenter_experiments__v1
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

    monitoring_derived__average_ping_sizes__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    monitoring_derived__bigquery_usage__v2.set_upstream(
        monitoring_derived__jobs_by_organization__v1
    )

    wait_for_copy_deduplicate_main_ping = ExternalTaskSensor(
        task_id="wait_for_copy_deduplicate_main_ping",
        external_dag_id="copy_deduplicate",
        external_task_id="copy_deduplicate_main_ping",
        execution_delta=datetime.timedelta(seconds=3600),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    monitoring_derived__column_size__v1.set_upstream(
        wait_for_copy_deduplicate_main_ping
    )

    monitoring_derived__event_error_monitoring_aggregates__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    monitoring_derived__event_flow_monitoring_aggregates__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    monitoring_derived__event_monitoring_aggregates__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    monitoring_derived__stable_and_derived_table_sizes__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )
    monitoring_derived__stable_and_derived_table_sizes__v1.set_upstream(
        wait_for_copy_deduplicate_main_ping
    )

    monitoring_derived__stable_table_column_counts__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )
    monitoring_derived__stable_table_column_counts__v1.set_upstream(
        wait_for_copy_deduplicate_main_ping
    )

    monitoring_derived__structured_missing_columns__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    monitoring_derived__telemetry_missing_columns__v3.set_upstream(
        wait_for_copy_deduplicate_all
    )
