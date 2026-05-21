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
### bqetl_statcounter_browser_market_share

Built from bigquery-etl repo, [`dags/bqetl_statcounter_browser_market_share.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_statcounter_browser_market_share.py)

#### Description

Pulls browser market share data from Statcounter CSV downloads

#### Owner

kbammarito@mozilla.com

#### Tags

* impact/tier_3
* repo/bigquery-etl
"""


default_args = {
    "owner": "kbammarito@mozilla.com",
    "start_date": datetime.datetime(2026, 5, 1, 0, 0),
    "end_date": None,
    "email": ["telemetry-alerts@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
    "max_active_tis_per_dag": None,
}

tags = ["impact/tier_3", "repo/bigquery-etl"]

with DAG(
    "bqetl_statcounter_browser_market_share",
    default_args=default_args,
    schedule_interval="0 10 * * *",
    doc_md=docs,
    tags=tags,
    catchup=False,
) as dag:

    checks__warn_statcounter_derived__browser_market_share_regions__v1 = bigquery_dq_check(
        task_id="checks__warn_statcounter_derived__browser_market_share_regions__v1",
        source_table="browser_market_share_regions_v1",
        dataset_id="statcounter_derived",
        project_id="moz-fx-data-shared-prod",
        is_dq_check_fail=False,
        owner="kbammarito@mozilla.com",
        email=["kbammarito@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        arguments=["--date-from", "{{ds}}"],
        parameters=["date:DATE:{{ds}}"],
        retry_delay=datetime.timedelta(seconds=300),
        retries=1,
    )

    checks__warn_statcounter_derived__browser_market_share_worldwide__v1 = bigquery_dq_check(
        task_id="checks__warn_statcounter_derived__browser_market_share_worldwide__v1",
        source_table="browser_market_share_worldwide_v1",
        dataset_id="statcounter_derived",
        project_id="moz-fx-data-shared-prod",
        is_dq_check_fail=False,
        owner="kbammarito@mozilla.com",
        email=["kbammarito@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        arguments=["--date-from", "{{ds}}"],
        parameters=["date:DATE:{{ds}}"],
        retry_delay=datetime.timedelta(seconds=300),
        retries=1,
    )

    statcounter_derived__browser_market_share_regions__v1 = GKEPodOperator(
        task_id="statcounter_derived__browser_market_share_regions__v1",
        arguments=[
            "python",
            "sql/moz-fx-data-shared-prod/statcounter_derived/browser_market_share_regions_v1/query.py",
        ]
        + ["--date-from", "{{ds}}"],
        image="us-docker.pkg.dev/moz-fx-data-artifacts-prod/bigquery-etl/bigquery-etl:latest",
        owner="kbammarito@mozilla.com",
        email=["kbammarito@mozilla.com", "telemetry-alerts@mozilla.com"],
    )

    statcounter_derived__browser_market_share_worldwide__v1 = GKEPodOperator(
        task_id="statcounter_derived__browser_market_share_worldwide__v1",
        arguments=[
            "python",
            "sql/moz-fx-data-shared-prod/statcounter_derived/browser_market_share_worldwide_v1/query.py",
        ]
        + ["--date-from", "{{ds}}"],
        image="us-docker.pkg.dev/moz-fx-data-artifacts-prod/bigquery-etl/bigquery-etl:latest",
        owner="kbammarito@mozilla.com",
        email=["kbammarito@mozilla.com", "telemetry-alerts@mozilla.com"],
    )

    checks__warn_statcounter_derived__browser_market_share_regions__v1.set_upstream(
        statcounter_derived__browser_market_share_regions__v1
    )

    checks__warn_statcounter_derived__browser_market_share_worldwide__v1.set_upstream(
        statcounter_derived__browser_market_share_worldwide__v1
    )
