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
### bqetl_desktop_conv_evnt_categorization

Built from bigquery-etl repo, [`dags/bqetl_desktop_conv_evnt_categorization.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_desktop_conv_evnt_categorization.py)

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
    "start_date": datetime.datetime(2024, 6, 4, 0, 0),
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
    "bqetl_desktop_conv_evnt_categorization",
    default_args=default_args,
    schedule_interval="0 12 * * *",
    doc_md=docs,
    tags=tags,
) as dag:

    wait_for_checks__fail_telemetry_derived__clients_last_seen__v2 = ExternalTaskSensor(
        task_id="wait_for_checks__fail_telemetry_derived__clients_last_seen__v2",
        external_dag_id="bqetl_main_summary",
        external_task_id="checks__fail_telemetry_derived__clients_last_seen__v2",
        execution_delta=datetime.timedelta(seconds=36000),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_clients_first_seen_v3 = ExternalTaskSensor(
        task_id="wait_for_clients_first_seen_v3",
        external_dag_id="bqetl_analytics_tables",
        external_task_id="clients_first_seen_v3",
        execution_delta=datetime.timedelta(seconds=36000),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_telemetry_derived__clients_first_seen__v1 = ExternalTaskSensor(
        task_id="wait_for_telemetry_derived__clients_first_seen__v1",
        external_dag_id="bqetl_main_summary",
        external_task_id="telemetry_derived__clients_first_seen__v1",
        execution_delta=datetime.timedelta(seconds=36000),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    checks__warn_google_ads_derived__conversion_event_categorization__v1 = bigquery_dq_check(
        task_id="checks__warn_google_ads_derived__conversion_event_categorization__v1",
        source_table='conversion_event_categorization_v1${{ macros.ds_format(macros.ds_add(ds, -14), "%Y-%m-%d", "%Y%m%d") }}',
        dataset_id="google_ads_derived",
        project_id="moz-fx-data-shared-prod",
        is_dq_check_fail=False,
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        parameters=["report_date:DATE:{{macros.ds_add(ds, -14)}}"]
        + ["submission_date:DATE:{{ds}}"],
        retries=0,
    )

    google_ads_derived__conversion_event_categorization__v1 = bigquery_etl_query(
        task_id="google_ads_derived__conversion_event_categorization__v1",
        destination_table='conversion_event_categorization_v1${{ macros.ds_format(macros.ds_add(ds, -14), "%Y-%m-%d", "%Y%m%d") }}',
        dataset_id="google_ads_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        parameters=["report_date:DATE:{{macros.ds_add(ds, -14)}}"]
        + ["submission_date:DATE:{{ds}}"],
    )

    with TaskGroup(
        "google_ads_derived__conversion_event_categorization__v1_external",
    ) as google_ads_derived__conversion_event_categorization__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_google_analytics_derived_ga4__wait_for_google_ads_derived__conversion_event_categorization__v1",
            external_dag_id="bqetl_google_analytics_derived_ga4",
            external_task_id="wait_for_google_ads_derived__conversion_event_categorization__v1",
        )

        google_ads_derived__conversion_event_categorization__v1_external.set_upstream(
            google_ads_derived__conversion_event_categorization__v1
        )

    checks__warn_google_ads_derived__conversion_event_categorization__v1.set_upstream(
        google_ads_derived__conversion_event_categorization__v1
    )

    google_ads_derived__conversion_event_categorization__v1.set_upstream(
        wait_for_checks__fail_telemetry_derived__clients_last_seen__v2
    )

    google_ads_derived__conversion_event_categorization__v1.set_upstream(
        wait_for_clients_first_seen_v3
    )

    google_ads_derived__conversion_event_categorization__v1.set_upstream(
        wait_for_telemetry_derived__clients_first_seen__v1
    )
