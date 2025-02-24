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
### bqetl_shredder_monitoring

Built from bigquery-etl repo, [`dags/bqetl_shredder_monitoring.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_shredder_monitoring.py)

#### Description

[EXPERIMENTAL] Monitoring queries for shredder operation
#### Owner

bewu@mozilla.com

#### Tags

* impact/tier_3
* repo/bigquery-etl
* triage/no_triage
"""


default_args = {
    "owner": "bewu@mozilla.com",
    "start_date": datetime.datetime(2024, 10, 1, 0, 0),
    "end_date": None,
    "email": ["bewu@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
}

tags = ["impact/tier_3", "repo/bigquery-etl", "triage/no_triage"]

with DAG(
    "bqetl_shredder_monitoring",
    default_args=default_args,
    schedule_interval="0 12 * * *",
    doc_md=docs,
    tags=tags,
    catchup=False,
) as dag:

    wait_for_monitoring_derived__bigquery_table_storage__v1 = ExternalTaskSensor(
        task_id="wait_for_monitoring_derived__bigquery_table_storage__v1",
        external_dag_id="bqetl_monitoring",
        external_task_id="monitoring_derived__bigquery_table_storage__v1",
        execution_delta=datetime.timedelta(seconds=36000),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_monitoring_derived__bigquery_tables_inventory__v1 = ExternalTaskSensor(
        task_id="wait_for_monitoring_derived__bigquery_tables_inventory__v1",
        external_dag_id="bqetl_monitoring",
        external_task_id="monitoring_derived__bigquery_tables_inventory__v1",
        execution_delta=datetime.timedelta(seconds=36000),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_monitoring_derived__jobs_by_organization__v1 = ExternalTaskSensor(
        task_id="wait_for_monitoring_derived__jobs_by_organization__v1",
        external_dag_id="bqetl_monitoring",
        external_task_id="monitoring_derived__jobs_by_organization__v1",
        execution_delta=datetime.timedelta(seconds=36000),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    monitoring_derived__shredder_targets__v1 = GKEPodOperator(
        task_id="monitoring_derived__shredder_targets__v1",
        arguments=[
            "python",
            "sql/moz-fx-data-shared-prod/monitoring_derived/shredder_targets_v1/query.py",
        ]
        + [
            "--output-table",
            "moz-fx-data-shared-prod.monitoring_derived.shredder_targets_v1",
            "--run-date",
            "{{ ds }}",
        ],
        image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
        owner="bewu@mozilla.com",
        email=["bewu@mozilla.com"],
    )

    monitoring_derived__shredder_targets_alert__v1 = GKEPodOperator(
        task_id="monitoring_derived__shredder_targets_alert__v1",
        arguments=[
            "python",
            "sql/moz-fx-data-shared-prod/monitoring_derived/shredder_targets_alert_v1/query.py",
        ]
        + ["--run-date", "{{ ds }}"],
        image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
        owner="bewu@mozilla.com",
        email=["bewu@mozilla.com"],
    )

    monitoring_derived__shredder_targets_joined__v1 = bigquery_etl_query(
        task_id="monitoring_derived__shredder_targets_joined__v1",
        destination_table="shredder_targets_joined_v1",
        dataset_id="monitoring_derived",
        project_id="moz-fx-data-shared-prod",
        owner="bewu@mozilla.com",
        email=["bewu@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    monitoring_derived__shredder_targets_new_mismatched__v1 = bigquery_etl_query(
        task_id="monitoring_derived__shredder_targets_new_mismatched__v1",
        destination_table="shredder_targets_new_mismatched_v1",
        dataset_id="monitoring_derived",
        project_id="moz-fx-data-shared-prod",
        owner="bewu@mozilla.com",
        email=["bewu@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    monitoring_derived__shredder_targets_alert__v1.set_upstream(
        monitoring_derived__shredder_targets_new_mismatched__v1
    )

    monitoring_derived__shredder_targets_joined__v1.set_upstream(
        wait_for_monitoring_derived__bigquery_table_storage__v1
    )

    monitoring_derived__shredder_targets_joined__v1.set_upstream(
        wait_for_monitoring_derived__bigquery_tables_inventory__v1
    )

    monitoring_derived__shredder_targets_joined__v1.set_upstream(
        wait_for_monitoring_derived__jobs_by_organization__v1
    )

    monitoring_derived__shredder_targets_joined__v1.set_upstream(
        monitoring_derived__shredder_targets__v1
    )

    monitoring_derived__shredder_targets_new_mismatched__v1.set_upstream(
        monitoring_derived__shredder_targets_joined__v1
    )
