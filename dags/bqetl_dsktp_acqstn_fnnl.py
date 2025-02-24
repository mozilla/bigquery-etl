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
### bqetl_dsktp_acqstn_fnnl

Built from bigquery-etl repo, [`dags/bqetl_dsktp_acqstn_fnnl.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_dsktp_acqstn_fnnl.py)

#### Description

This DAG builds the Desktop Acquisiton Funnel aggregate table

#### Owner

kwindau@mozilla.com

#### Tags

* impact/tier_2
* repo/bigquery-etl
"""


default_args = {
    "owner": "kwindau@mozilla.com",
    "start_date": datetime.datetime(2025, 1, 8, 0, 0),
    "end_date": None,
    "email": ["telemetry-alerts@mozilla.com", "kwindau@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=300),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
}

tags = ["impact/tier_2", "repo/bigquery-etl"]

with DAG(
    "bqetl_dsktp_acqstn_fnnl",
    default_args=default_args,
    schedule_interval="30 11 * * *",
    doc_md=docs,
    tags=tags,
    catchup=False,
) as dag:

    wait_for_telemetry_derived__clients_first_seen_28_days_later__v1 = (
        ExternalTaskSensor(
            task_id="wait_for_telemetry_derived__clients_first_seen_28_days_later__v1",
            external_dag_id="bqetl_analytics_tables",
            external_task_id="telemetry_derived__clients_first_seen_28_days_later__v1",
            execution_delta=datetime.timedelta(seconds=34200),
            check_existence=True,
            mode="reschedule",
            poke_interval=datetime.timedelta(minutes=5),
            allowed_states=ALLOWED_STATES,
            failed_states=FAILED_STATES,
            pool="DATA_ENG_EXTERNALTASKSENSOR",
        )
    )

    checks__fail_telemetry_derived__desktop_acquisition_funnel_aggregates__v1 = bigquery_dq_check(
        task_id="checks__fail_telemetry_derived__desktop_acquisition_funnel_aggregates__v1",
        source_table='desktop_acquisition_funnel_aggregates_v1${{ macros.ds_format(macros.ds_add(ds, -29), "%Y-%m-%d", "%Y%m%d") }}',
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        is_dq_check_fail=True,
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        parameters=["fsd:DATE:{{macros.ds_add(ds, -29)}}"]
        + ["submission_date:DATE:{{ds}}"],
        retries=0,
    )

    telemetry_derived__desktop_acquisition_funnel_aggregates__v1 = bigquery_etl_query(
        task_id="telemetry_derived__desktop_acquisition_funnel_aggregates__v1",
        destination_table='desktop_acquisition_funnel_aggregates_v1${{ macros.ds_format(macros.ds_add(ds, -29), "%Y-%m-%d", "%Y%m%d") }}',
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        parameters=["fsd:DATE:{{macros.ds_add(ds, -29)}}"]
        + ["submission_date:DATE:{{ds}}"],
    )

    checks__fail_telemetry_derived__desktop_acquisition_funnel_aggregates__v1.set_upstream(
        telemetry_derived__desktop_acquisition_funnel_aggregates__v1
    )

    telemetry_derived__desktop_acquisition_funnel_aggregates__v1.set_upstream(
        wait_for_telemetry_derived__clients_first_seen_28_days_later__v1
    )
