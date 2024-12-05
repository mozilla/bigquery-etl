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
### bqetl_ios_campaign_reporting

Built from bigquery-etl repo, [`dags/bqetl_ios_campaign_reporting.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_ios_campaign_reporting.py)

#### Description

Loads the apple ads ios_app_campaign_stats table
#### Owner

kwindau@mozilla.com

#### Tags

* impact/tier_2
* repo/bigquery-etl
"""


default_args = {
    "owner": "kwindau@mozilla.com",
    "start_date": datetime.datetime(2024, 5, 8, 0, 0),
    "end_date": None,
    "email": ["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
}

tags = ["impact/tier_2", "repo/bigquery-etl"]

with DAG(
    "bqetl_ios_campaign_reporting",
    default_args=default_args,
    schedule_interval="0 12 * * *",
    doc_md=docs,
    tags=tags,
) as dag:

    wait_for_apple_ads_external__ad_group_report__v1 = ExternalTaskSensor(
        task_id="wait_for_apple_ads_external__ad_group_report__v1",
        external_dag_id="bqetl_fivetran_apple_ads",
        external_task_id="apple_ads_external__ad_group_report__v1",
        execution_delta=datetime.timedelta(seconds=32400),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_checks__fail_firefox_ios_derived__clients_activation__v1 = (
        ExternalTaskSensor(
            task_id="wait_for_checks__fail_firefox_ios_derived__clients_activation__v1",
            external_dag_id="bqetl_firefox_ios",
            external_task_id="checks__fail_firefox_ios_derived__clients_activation__v1",
            execution_delta=datetime.timedelta(seconds=28800),
            check_existence=True,
            mode="reschedule",
            allowed_states=ALLOWED_STATES,
            failed_states=FAILED_STATES,
            pool="DATA_ENG_EXTERNALTASKSENSOR",
        )
    )

    wait_for_checks__fail_firefox_ios_derived__firefox_ios_clients__v1 = ExternalTaskSensor(
        task_id="wait_for_checks__fail_firefox_ios_derived__firefox_ios_clients__v1",
        external_dag_id="bqetl_firefox_ios",
        external_task_id="checks__fail_firefox_ios_derived__firefox_ios_clients__v1",
        execution_delta=datetime.timedelta(seconds=28800),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_firefox_ios_derived__funnel_retention_week_4__v1 = ExternalTaskSensor(
        task_id="wait_for_firefox_ios_derived__funnel_retention_week_4__v1",
        external_dag_id="bqetl_firefox_ios",
        external_task_id="firefox_ios_derived__funnel_retention_week_4__v1",
        execution_delta=datetime.timedelta(seconds=28800),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    apple_ads_external__ios_app_campaign_stats__v1 = bigquery_etl_query(
        task_id="apple_ads_external__ios_app_campaign_stats__v1",
        destination_table='ios_app_campaign_stats_v1${{ macros.ds_format(macros.ds_add(ds, -27), "%Y-%m-%d", "%Y%m%d") }}',
        dataset_id="apple_ads_external",
        project_id="moz-fx-data-shared-prod",
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        parameters=["date:DATE:{{macros.ds_add(ds, -27)}}"]
        + ["submission_date:DATE:{{ds}}"],
    )

    apple_ads_external__ios_app_campaign_stats__v1.set_upstream(
        wait_for_apple_ads_external__ad_group_report__v1
    )

    apple_ads_external__ios_app_campaign_stats__v1.set_upstream(
        wait_for_checks__fail_firefox_ios_derived__clients_activation__v1
    )

    apple_ads_external__ios_app_campaign_stats__v1.set_upstream(
        wait_for_checks__fail_firefox_ios_derived__firefox_ios_clients__v1
    )

    apple_ads_external__ios_app_campaign_stats__v1.set_upstream(
        wait_for_firefox_ios_derived__funnel_retention_week_4__v1
    )
