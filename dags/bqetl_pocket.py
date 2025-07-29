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
### bqetl_pocket

Built from bigquery-etl repo, [`dags/bqetl_pocket.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_pocket.py)

#### Description

Import of data from Pocket's Snowflake warehouse.

Originally created for [Bug 1695336](
https://bugzilla.mozilla.org/show_bug.cgi?id=1695336).

*Triage notes*

As long as the most recent DAG run is successful this job can be considered healthy.
In such case, past DAG failures can be ignored.

#### Owner

kik@mozilla.com

#### Tags

* impact/tier_2
* repo/bigquery-etl
"""


default_args = {
    "owner": "kik@mozilla.com",
    "start_date": datetime.datetime(2021, 3, 10, 0, 0),
    "end_date": None,
    "email": ["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=3600),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 10,
    "max_active_tis_per_dag": None,
}

tags = ["impact/tier_2", "repo/bigquery-etl"]

with DAG(
    "bqetl_pocket",
    default_args=default_args,
    schedule_interval="0 12 * * *",
    doc_md=docs,
    tags=tags,
    catchup=False,
) as dag:

    wait_for_copy_deduplicate_all = ExternalTaskSensor(
        task_id="wait_for_copy_deduplicate_all",
        external_dag_id="copy_deduplicate",
        external_task_id="copy_deduplicate_all",
        execution_delta=datetime.timedelta(seconds=39600),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    pocket_derived__pocket_usage__v1 = bigquery_etl_query(
        task_id="pocket_derived__pocket_usage__v1",
        destination_table="pocket_usage_v1",
        dataset_id="pocket_derived",
        project_id="moz-fx-data-shared-prod",
        owner="gkatre@mozilla.com",
        email=["gkatre@mozilla.com", "kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    pocket_derived__spoc_tile_ids__v1 = bigquery_etl_query(
        task_id="pocket_derived__spoc_tile_ids__v1",
        destination_table="spoc_tile_ids_v1",
        dataset_id="pocket_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
        parameters=["submission_date:DATE:{{ds}}"],
    )

    pocket_derived__spoc_tile_ids_history__v1 = GKEPodOperator(
        task_id="pocket_derived__spoc_tile_ids_history__v1",
        arguments=[
            "python",
            "sql/moz-fx-data-shared-prod/pocket_derived/spoc_tile_ids_history_v1/query.py",
        ]
        + ["--date", "{{ ds }}"],
        image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
    )

    pocket_derived__pocket_usage__v1.set_upstream(wait_for_copy_deduplicate_all)

    pocket_derived__spoc_tile_ids__v1.set_upstream(
        pocket_derived__spoc_tile_ids_history__v1
    )
