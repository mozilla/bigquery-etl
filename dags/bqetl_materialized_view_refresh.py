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
### bqetl_materialized_view_refresh

Built from bigquery-etl repo, [`dags/bqetl_materialized_view_refresh.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_materialized_view_refresh.py)

#### Description

Manual refreshes of materialized views.  See https://mozilla-hub.atlassian.net/browse/DENG-6990

*Triage notes*

Transient errors may occur.  The DAG is fine long as the latest run worked.

#### Owner

bewu@mozilla.com

#### Tags

* impact/tier_1
* repo/bigquery-etl
"""


default_args = {
    "owner": "bewu@mozilla.com",
    "start_date": datetime.datetime(2025, 3, 21, 0, 0),
    "end_date": None,
    "email": ["telemetry-alerts@mozilla.com", "bewu@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=300),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
    "max_active_tis_per_dag": 1,
}

tags = ["impact/tier_1", "repo/bigquery-etl"]

with DAG(
    "bqetl_materialized_view_refresh",
    default_args=default_args,
    schedule_interval="45 * * * *",
    doc_md=docs,
    tags=tags,
    catchup=False,
) as dag:

    firefox_desktop_derived__event_monitoring_live__v1 = bigquery_etl_query(
        task_id="firefox_desktop_derived__event_monitoring_live__v1",
        destination_table=None,
        dataset_id="firefox_desktop_derived",
        project_id="moz-fx-data-backfill-3",
        owner="bewu@mozilla.com",
        email=["bewu@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        arguments=["--use_legacy_sql=false"],
        sql_file_path="sql/moz-fx-data-backfill-3/firefox_desktop_derived/event_monitoring_live_v1/script.sql",
    )
