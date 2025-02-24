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
### bqetl_glam_refresh_aggregates

Built from bigquery-etl repo, [`dags/bqetl_glam_refresh_aggregates.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_glam_refresh_aggregates.py)

#### Description

Refresh GLAM tables that are serving data.
#### Owner

efilho@mozilla.com

#### Tags

* impact/tier_2
* repo/bigquery-etl
"""


default_args = {
    "owner": "efilho@mozilla.com",
    "start_date": datetime.datetime(2024, 1, 10, 0, 0),
    "end_date": None,
    "email": ["efilho@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
}

tags = ["impact/tier_2", "repo/bigquery-etl"]

with DAG(
    "bqetl_glam_refresh_aggregates",
    default_args=default_args,
    schedule_interval="0 8 * * *",
    doc_md=docs,
    tags=tags,
    catchup=False,
) as dag:

    wait_for_glam_client_probe_counts_beta_extract = ExternalTaskSensor(
        task_id="wait_for_glam_client_probe_counts_beta_extract",
        external_dag_id="glam",
        external_task_id="extracts.glam_client_probe_counts_beta_extract",
        execution_delta=datetime.timedelta(days=-1, seconds=57600),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_glam_client_probe_counts_nightly_extract = ExternalTaskSensor(
        task_id="wait_for_glam_client_probe_counts_nightly_extract",
        external_dag_id="glam",
        external_task_id="extracts.glam_client_probe_counts_nightly_extract",
        execution_delta=datetime.timedelta(days=-1, seconds=57600),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_glam_client_probe_counts_release_extract = ExternalTaskSensor(
        task_id="wait_for_glam_client_probe_counts_release_extract",
        external_dag_id="glam",
        external_task_id="extracts.glam_client_probe_counts_release_extract",
        execution_delta=datetime.timedelta(days=-1, seconds=57600),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_query_org_mozilla_fenix_glam_beta__extract_probe_counts_v1 = ExternalTaskSensor(
        task_id="wait_for_query_org_mozilla_fenix_glam_beta__extract_probe_counts_v1",
        external_dag_id="glam_fenix",
        external_task_id="query_org_mozilla_fenix_glam_beta__extract_probe_counts_v1",
        execution_delta=datetime.timedelta(seconds=21600),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_query_org_mozilla_fenix_glam_nightly__extract_probe_counts_v1 = ExternalTaskSensor(
        task_id="wait_for_query_org_mozilla_fenix_glam_nightly__extract_probe_counts_v1",
        external_dag_id="glam_fenix",
        external_task_id="query_org_mozilla_fenix_glam_nightly__extract_probe_counts_v1",
        execution_delta=datetime.timedelta(seconds=21600),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_firefox_desktop_glam_beta_done = ExternalTaskSensor(
        task_id="wait_for_firefox_desktop_glam_beta_done",
        external_dag_id="glam_fog",
        external_task_id="firefox_desktop_glam_beta_done",
        execution_delta=datetime.timedelta(seconds=21600),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_firefox_desktop_glam_nightly_done = ExternalTaskSensor(
        task_id="wait_for_firefox_desktop_glam_nightly_done",
        external_dag_id="glam_fog",
        external_task_id="firefox_desktop_glam_nightly_done",
        execution_delta=datetime.timedelta(seconds=21600),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    glam_etl__glam_desktop_beta_aggregates__v1 = bigquery_etl_query(
        task_id="glam_etl__glam_desktop_beta_aggregates__v1",
        destination_table=None,
        dataset_id="glam_etl",
        project_id="moz-fx-glam-prod",
        owner="efilho@mozilla.com",
        email=["efilho@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        sql_file_path="sql/moz-fx-glam-prod/glam_etl/glam_desktop_beta_aggregates_v1/script.sql",
    )

    glam_etl__glam_desktop_nightly_aggregates__v1 = bigquery_etl_query(
        task_id="glam_etl__glam_desktop_nightly_aggregates__v1",
        destination_table=None,
        dataset_id="glam_etl",
        project_id="moz-fx-glam-prod",
        owner="efilho@mozilla.com",
        email=["efilho@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        sql_file_path="sql/moz-fx-glam-prod/glam_etl/glam_desktop_nightly_aggregates_v1/script.sql",
    )

    glam_etl__glam_desktop_release_aggregates__v1 = bigquery_etl_query(
        task_id="glam_etl__glam_desktop_release_aggregates__v1",
        destination_table=None,
        dataset_id="glam_etl",
        project_id="moz-fx-glam-prod",
        owner="efilho@mozilla.com",
        email=["efilho@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        sql_file_path="sql/moz-fx-glam-prod/glam_etl/glam_desktop_release_aggregates_v1/script.sql",
    )

    glam_etl__glam_fenix_beta_aggregates__v1 = bigquery_etl_query(
        task_id="glam_etl__glam_fenix_beta_aggregates__v1",
        destination_table=None,
        dataset_id="glam_etl",
        project_id="moz-fx-glam-prod",
        owner="efilho@mozilla.com",
        email=["efilho@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        sql_file_path="sql/moz-fx-glam-prod/glam_etl/glam_fenix_beta_aggregates_v1/script.sql",
    )

    glam_etl__glam_fenix_nightly_aggregates__v1 = bigquery_etl_query(
        task_id="glam_etl__glam_fenix_nightly_aggregates__v1",
        destination_table=None,
        dataset_id="glam_etl",
        project_id="moz-fx-glam-prod",
        owner="efilho@mozilla.com",
        email=["efilho@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        sql_file_path="sql/moz-fx-glam-prod/glam_etl/glam_fenix_nightly_aggregates_v1/script.sql",
    )

    glam_etl__glam_fog_beta_aggregates__v1 = bigquery_etl_query(
        task_id="glam_etl__glam_fog_beta_aggregates__v1",
        destination_table=None,
        dataset_id="glam_etl",
        project_id="moz-fx-glam-prod",
        owner="efilho@mozilla.com",
        email=["efilho@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        sql_file_path="sql/moz-fx-glam-prod/glam_etl/glam_fog_beta_aggregates_v1/script.sql",
    )

    glam_etl__glam_fog_nightly_aggregates__v1 = bigquery_etl_query(
        task_id="glam_etl__glam_fog_nightly_aggregates__v1",
        destination_table=None,
        dataset_id="glam_etl",
        project_id="moz-fx-glam-prod",
        owner="efilho@mozilla.com",
        email=["efilho@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        sql_file_path="sql/moz-fx-glam-prod/glam_etl/glam_fog_nightly_aggregates_v1/script.sql",
    )

    glam_etl__glam_desktop_beta_aggregates__v1.set_upstream(
        wait_for_glam_client_probe_counts_beta_extract
    )

    glam_etl__glam_desktop_nightly_aggregates__v1.set_upstream(
        wait_for_glam_client_probe_counts_nightly_extract
    )

    glam_etl__glam_desktop_release_aggregates__v1.set_upstream(
        wait_for_glam_client_probe_counts_release_extract
    )

    glam_etl__glam_fenix_beta_aggregates__v1.set_upstream(
        wait_for_query_org_mozilla_fenix_glam_beta__extract_probe_counts_v1
    )

    glam_etl__glam_fenix_nightly_aggregates__v1.set_upstream(
        wait_for_query_org_mozilla_fenix_glam_nightly__extract_probe_counts_v1
    )

    glam_etl__glam_fog_beta_aggregates__v1.set_upstream(
        wait_for_firefox_desktop_glam_beta_done
    )

    glam_etl__glam_fog_nightly_aggregates__v1.set_upstream(
        wait_for_firefox_desktop_glam_nightly_done
    )
