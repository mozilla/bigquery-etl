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
### bqetl_nimbus_feature_monitoring

Built from bigquery-etl repo, [`dags/bqetl_nimbus_feature_monitoring.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_nimbus_feature_monitoring.py)

#### Description

The DAG schedules nimbus feature monitoring queries.
#### Owner

project-nimbus@mozilla.com

#### Tags

* impact/tier_2
* repo/bigquery-etl
"""


default_args = {
    "owner": "project-nimbus@mozilla.com",
    "start_date": datetime.datetime(2026, 3, 16, 0, 0),
    "end_date": None,
    "email": ["telemetry-alerts@mozilla.com", "project-nimbus@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "max_active_tis_per_dag": None,
}

tags = ["impact/tier_2", "repo/bigquery-etl"]

with DAG(
    "bqetl_nimbus_feature_monitoring",
    default_args=default_args,
    schedule_interval="0 3 * * *",
    doc_md=docs,
    tags=tags,
    catchup=False,
) as dag:

    wait_for_copy_deduplicate_all = ExternalTaskSensor(
        task_id="wait_for_copy_deduplicate_all",
        external_dag_id="copy_deduplicate",
        external_task_id="copy_deduplicate_all",
        execution_delta=datetime.timedelta(seconds=7200),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_firefox_desktop_derived__events_stream__v1 = ExternalTaskSensor(
        task_id="wait_for_firefox_desktop_derived__events_stream__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="firefox_desktop.firefox_desktop_derived__events_stream__v1",
        execution_delta=datetime.timedelta(seconds=3600),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_monitoring__experimenter_experiments__v1 = ExternalTaskSensor(
        task_id="wait_for_monitoring__experimenter_experiments__v1",
        external_dag_id="bqetl_experimenter_experiments_import",
        external_task_id="monitoring__experimenter_experiments__v1",
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    firefox_desktop_derived__nimbus_feature_monitoring_address_autofill_feature__v1 = bigquery_etl_query(
        task_id="firefox_desktop_derived__nimbus_feature_monitoring_address_autofill_feature__v1",
        destination_table="nimbus_feature_monitoring_address_autofill_feature_v1",
        dataset_id="firefox_desktop_derived",
        project_id="moz-fx-data-shared-prod",
        owner="project-nimbus@mozilla.com",
        email=["project-nimbus@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    firefox_desktop_derived__nimbus_feature_monitoring_newtabsectionsexperiment__v1 = bigquery_etl_query(
        task_id="firefox_desktop_derived__nimbus_feature_monitoring_newtabsectionsexperiment__v1",
        destination_table="nimbus_feature_monitoring_newtabsectionsexperiment_v1",
        dataset_id="firefox_desktop_derived",
        project_id="moz-fx-data-shared-prod",
        owner="project-nimbus@mozilla.com",
        email=["project-nimbus@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    firefox_desktop_derived__nimbus_feature_monitoring_newtabsponsoredcontent__v1 = bigquery_etl_query(
        task_id="firefox_desktop_derived__nimbus_feature_monitoring_newtabsponsoredcontent__v1",
        destination_table="nimbus_feature_monitoring_newtabsponsoredcontent_v1",
        dataset_id="firefox_desktop_derived",
        project_id="moz-fx-data-shared-prod",
        owner="project-nimbus@mozilla.com",
        email=["project-nimbus@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    firefox_desktop_derived__nimbus_feature_monitoring_newtabtrainhopaddon__v1 = bigquery_etl_query(
        task_id="firefox_desktop_derived__nimbus_feature_monitoring_newtabtrainhopaddon__v1",
        destination_table="nimbus_feature_monitoring_newtabtrainhopaddon_v1",
        dataset_id="firefox_desktop_derived",
        project_id="moz-fx-data-shared-prod",
        owner="project-nimbus@mozilla.com",
        email=["project-nimbus@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    firefox_desktop_derived__nimbus_feature_monitoring_address_autofill_feature__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    firefox_desktop_derived__nimbus_feature_monitoring_address_autofill_feature__v1.set_upstream(
        wait_for_firefox_desktop_derived__events_stream__v1
    )

    firefox_desktop_derived__nimbus_feature_monitoring_address_autofill_feature__v1.set_upstream(
        wait_for_monitoring__experimenter_experiments__v1
    )

    firefox_desktop_derived__nimbus_feature_monitoring_newtabsectionsexperiment__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    firefox_desktop_derived__nimbus_feature_monitoring_newtabsectionsexperiment__v1.set_upstream(
        wait_for_monitoring__experimenter_experiments__v1
    )

    firefox_desktop_derived__nimbus_feature_monitoring_newtabsponsoredcontent__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    firefox_desktop_derived__nimbus_feature_monitoring_newtabsponsoredcontent__v1.set_upstream(
        wait_for_monitoring__experimenter_experiments__v1
    )

    firefox_desktop_derived__nimbus_feature_monitoring_newtabtrainhopaddon__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    firefox_desktop_derived__nimbus_feature_monitoring_newtabtrainhopaddon__v1.set_upstream(
        wait_for_monitoring__experimenter_experiments__v1
    )
