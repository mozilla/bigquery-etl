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
### bqetl_census_feed

Built from bigquery-etl repo, [`dags/bqetl_census_feed.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_census_feed.py)

#### Description

Loads the desktop conversion event tables
#### Owner

kwindau@mozilla.com

#### Tags

* impact/tier_2
* repo/bigquery-etl
"""


default_args = {
    "owner": "kwindau@mozilla.com",
    "start_date": datetime.datetime(2024, 6, 10, 0, 0),
    "end_date": None,
    "email": ["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "max_active_tis_per_dag": None,
}

tags = ["impact/tier_2", "repo/bigquery-etl"]

with DAG(
    "bqetl_census_feed",
    default_args=default_args,
    schedule_interval="0 17 * * *",
    doc_md=docs,
    tags=tags,
    catchup=False,
) as dag:

    wait_for_checks__fail_mozilla_org_derived__gclid_conversions__v2 = (
        ExternalTaskSensor(
            task_id="wait_for_checks__fail_mozilla_org_derived__gclid_conversions__v2",
            external_dag_id="bqetl_google_analytics_derived_ga4",
            external_task_id="checks__fail_mozilla_org_derived__gclid_conversions__v2",
            execution_delta=datetime.timedelta(seconds=18000),
            check_existence=True,
            mode="reschedule",
            poke_interval=datetime.timedelta(minutes=5),
            allowed_states=ALLOWED_STATES,
            failed_states=FAILED_STATES,
            pool="DATA_ENG_EXTERNALTASKSENSOR",
        )
    )

    wait_for_checks__fail_mozilla_org_derived__gclid_conversions__v3 = (
        ExternalTaskSensor(
            task_id="wait_for_checks__fail_mozilla_org_derived__gclid_conversions__v3",
            external_dag_id="bqetl_google_analytics_derived_ga4",
            external_task_id="checks__fail_mozilla_org_derived__gclid_conversions__v3",
            execution_delta=datetime.timedelta(seconds=18000),
            check_existence=True,
            mode="reschedule",
            poke_interval=datetime.timedelta(minutes=5),
            allowed_states=ALLOWED_STATES,
            failed_states=FAILED_STATES,
            pool="DATA_ENG_EXTERNALTASKSENSOR",
        )
    )

    mozilla_org_derived__ga_desktop_conversions__v1 = bigquery_etl_query(
        task_id="mozilla_org_derived__ga_desktop_conversions__v1",
        destination_table='ga_desktop_conversions_v1${{ macros.ds_format(macros.ds_add(ds, -2), "%Y-%m-%d", "%Y%m%d") }}',
        dataset_id="mozilla_org_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        parameters=["activity_date:DATE:{{macros.ds_add(ds, -2)}}"]
        + ["submission_date:DATE:{{ds}}"],
    )

    mozilla_org_derived__ga_desktop_conversions__v2 = bigquery_etl_query(
        task_id="mozilla_org_derived__ga_desktop_conversions__v2",
        destination_table='ga_desktop_conversions_v2${{ macros.ds_format(macros.ds_add(ds, -2), "%Y-%m-%d", "%Y%m%d") }}',
        dataset_id="mozilla_org_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=True,
        parameters=["activity_date:DATE:{{macros.ds_add(ds, -2)}}"]
        + ["submission_date:DATE:{{ds}}"],
    )

    mozilla_org_derived__ga_desktop_conversions__v1.set_upstream(
        wait_for_checks__fail_mozilla_org_derived__gclid_conversions__v2
    )

    mozilla_org_derived__ga_desktop_conversions__v2.set_upstream(
        wait_for_checks__fail_mozilla_org_derived__gclid_conversions__v3
    )
