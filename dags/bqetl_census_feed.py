# Generated via https://github.com/mozilla/bigquery-etl/blob/main/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskMarker
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
from airflow.providers.cncf.kubernetes.secret import Secret
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

kik@mozilla.com

#### Tags

* impact/tier_2
* repo/bigquery-etl
"""

bigeye__firefoxdotcom_derived__fbclid_desktop_conversion_events_export__v1_bqetl_fb_access_token = Secret(
    deploy_type="env",
    deploy_target="FB_ACCESS_TOKEN",
    secret="airflow-gke-secrets",
    key="bqetl_fb_access_token",
)
bigeye__firefoxdotcom_derived__fbclid_desktop_conversion_events_export__v1_bqetl_fb_pixel_id = Secret(
    deploy_type="env",
    deploy_target="FB_PIXEL_ID",
    secret="airflow-gke-secrets",
    key="bqetl_fb_pixel_id",
)
firefoxdotcom_derived__fbclid_desktop_conversion_events_export__v1_bqetl_fb_access_token = Secret(
    deploy_type="env",
    deploy_target="FB_ACCESS_TOKEN",
    secret="airflow-gke-secrets",
    key="bqetl_fb_access_token",
)
firefoxdotcom_derived__fbclid_desktop_conversion_events_export__v1_bqetl_fb_pixel_id = (
    Secret(
        deploy_type="env",
        deploy_target="FB_PIXEL_ID",
        secret="airflow-gke-secrets",
        key="bqetl_fb_pixel_id",
    )
)


default_args = {
    "owner": "kik@mozilla.com",
    "start_date": datetime.datetime(2024, 6, 10, 0, 0),
    "end_date": None,
    "email": ["telemetry-alerts@mozilla.com"],
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

    wait_for_bigeye__firefoxdotcom_derived__fbclid_desktop_conversion_events__v1 = ExternalTaskSensor(
        task_id="wait_for_bigeye__firefoxdotcom_derived__fbclid_desktop_conversion_events__v1",
        external_dag_id="bqetl_ga4_firefoxdotcom",
        external_task_id="bigeye__firefoxdotcom_derived__fbclid_desktop_conversion_events__v1",
        execution_delta=datetime.timedelta(seconds=10800),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_bigeye__firefoxdotcom_derived__gclid_conversions__v1 = ExternalTaskSensor(
        task_id="wait_for_bigeye__firefoxdotcom_derived__gclid_conversions__v1",
        external_dag_id="bqetl_ga4_firefoxdotcom",
        external_task_id="bigeye__firefoxdotcom_derived__gclid_conversions__v1",
        execution_delta=datetime.timedelta(seconds=10800),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_firefoxdotcom_derived__glean_gclid_conversions__v1 = ExternalTaskSensor(
        task_id="wait_for_firefoxdotcom_derived__glean_gclid_conversions__v1",
        external_dag_id="bqetl_ga4_firefoxdotcom",
        external_task_id="firefoxdotcom_derived__glean_gclid_conversions__v1",
        execution_delta=datetime.timedelta(seconds=10800),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    bigeye__firefoxdotcom_derived__fbclid_desktop_conversion_events_export__v1 = bigquery_bigeye_check(
        task_id="bigeye__firefoxdotcom_derived__fbclid_desktop_conversion_events_export__v1",
        table_id="moz-fx-data-shared-prod.firefoxdotcom_derived.fbclid_desktop_conversion_events_export_v1",
        warehouse_id="1939",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
    )

    firefoxdotcom_derived__fbclid_desktop_conversion_events_export__v1 = GKEPodOperator(
        task_id="firefoxdotcom_derived__fbclid_desktop_conversion_events_export__v1",
        arguments=[
            "python",
            "sql/moz-fx-data-shared-prod/firefoxdotcom_derived/fbclid_desktop_conversion_events_export_v1/query.py",
        ]
        + ["--submission_date={{macros.ds_add(ds, -2)}}"],
        image="us-docker.pkg.dev/moz-fx-data-artifacts-prod/bigquery-etl/bigquery-etl:latest",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        secrets=[
            firefoxdotcom_derived__fbclid_desktop_conversion_events_export__v1_bqetl_fb_access_token,
            firefoxdotcom_derived__fbclid_desktop_conversion_events_export__v1_bqetl_fb_pixel_id,
        ],
    )

    firefoxdotcom_derived__ga_desktop_conversions__v1 = bigquery_etl_query(
        task_id="firefoxdotcom_derived__ga_desktop_conversions__v1",
        destination_table='ga_desktop_conversions_v1${{ macros.ds_format(macros.ds_add(ds, -2), "%Y-%m-%d", "%Y%m%d") }}',
        dataset_id="firefoxdotcom_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=True,
        parameters=["activity_date:DATE:{{macros.ds_add(ds, -2)}}"]
        + ["submission_date:DATE:{{ds}}"],
    )

    firefoxdotcom_derived__glean_ga_desktop_conversions__v1 = bigquery_etl_query(
        task_id="firefoxdotcom_derived__glean_ga_desktop_conversions__v1",
        destination_table="glean_ga_desktop_conversions_v1",
        dataset_id="firefoxdotcom_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kik@mozilla.com",
        email=["kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=True,
    )

    bigeye__firefoxdotcom_derived__fbclid_desktop_conversion_events_export__v1.set_upstream(
        firefoxdotcom_derived__fbclid_desktop_conversion_events_export__v1
    )

    firefoxdotcom_derived__fbclid_desktop_conversion_events_export__v1.set_upstream(
        wait_for_bigeye__firefoxdotcom_derived__fbclid_desktop_conversion_events__v1
    )

    firefoxdotcom_derived__ga_desktop_conversions__v1.set_upstream(
        wait_for_bigeye__firefoxdotcom_derived__gclid_conversions__v1
    )

    firefoxdotcom_derived__glean_ga_desktop_conversions__v1.set_upstream(
        wait_for_firefoxdotcom_derived__glean_gclid_conversions__v1
    )
