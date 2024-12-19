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
### bqetl_broken_reports_agg

Built from bigquery-etl repo, [`dags/bqetl_broken_reports_agg.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_broken_reports_agg.py)

#### Description

Tables associated with broken site reports.
#### Owner

gkatre@mozilla.com

#### Tags

* impact/tier_3
* repo/bigquery-etl
"""


default_args = {
    "owner": "gkatre@mozilla.com",
    "start_date": datetime.datetime(2024, 12, 18, 0, 0),
    "end_date": None,
    "email": ["telemetry-alerts@mozilla.com", "gkatre@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
}

tags = ["impact/tier_3", "repo/bigquery-etl"]

with DAG(
    "bqetl_broken_reports_agg",
    default_args=default_args,
    schedule_interval="0 6 * * *",
    doc_md=docs,
    tags=tags,
) as dag:

    wait_for_copy_deduplicate_all = ExternalTaskSensor(
        task_id="wait_for_copy_deduplicate_all",
        external_dag_id="copy_deduplicate",
        external_task_id="copy_deduplicate_all",
        execution_delta=datetime.timedelta(seconds=18000),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    org_mozilla_broken_site_report_derived__broken_site_root_domain_daily_aggregates__v1 = bigquery_etl_query(
        task_id="org_mozilla_broken_site_report_derived__broken_site_root_domain_daily_aggregates__v1",
        destination_table="broken_site_root_domain_daily_aggregates_v1",
        dataset_id="org_mozilla_broken_site_report_derived",
        project_id="moz-fx-data-shared-prod",
        owner="gkatre@mozilla.com",
        email=["gkatre@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    org_mozilla_broken_site_report_derived__broken_site_root_domain_weekly_trend__v1 = bigquery_etl_query(
        task_id="org_mozilla_broken_site_report_derived__broken_site_root_domain_weekly_trend__v1",
        destination_table="broken_site_root_domain_weekly_trend_v1",
        dataset_id="org_mozilla_broken_site_report_derived",
        project_id="moz-fx-data-shared-prod",
        owner="gkatre@mozilla.com",
        email=["gkatre@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=True,
    )

    org_mozilla_broken_site_report_derived__broken_site_root_domain_daily_aggregates__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    org_mozilla_broken_site_report_derived__broken_site_root_domain_weekly_trend__v1.set_upstream(
        org_mozilla_broken_site_report_derived__broken_site_root_domain_daily_aggregates__v1
    )
