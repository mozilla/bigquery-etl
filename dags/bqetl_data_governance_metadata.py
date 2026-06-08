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
### bqetl_data_governance_metadata

Built from bigquery-etl repo, [`dags/bqetl_data_governance_metadata.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_data_governance_metadata.py)

#### Description

Profiles columns of source datasets and writes per-column statistics to
data_governance_metadata_derived for schema enrichment tooling.

#### Owner

gkatre@mozilla.com

#### Tags

* impact/tier_3
* repo/bigquery-etl
"""


default_args = {
    "owner": "gkatre@mozilla.com",
    "start_date": datetime.datetime(2026, 6, 1, 0, 0),
    "end_date": None,
    "email": ["gkatre@mozilla.com", "telemetry-alerts@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
    "max_active_tis_per_dag": None,
}

tags = ["impact/tier_3", "repo/bigquery-etl"]

with DAG(
    "bqetl_data_governance_metadata",
    default_args=default_args,
    schedule_interval="0 4 * * 1",
    doc_md=docs,
    tags=tags,
    catchup=False,
) as dag:

    data_governance_metadata_derived__column_profiles__v1 = GKEPodOperator(
        task_id="data_governance_metadata_derived__column_profiles__v1",
        arguments=[
            "python",
            "sql/moz-fx-data-shared-prod/data_governance_metadata_derived/column_profiles_v1/query.py",
        ]
        + ["--date", "{{ ds }}", "--source-datasets", "telemetry_derived", "telemetry"],
        image="us-docker.pkg.dev/moz-fx-data-artifacts-prod/bigquery-etl/bigquery-etl:latest",
        owner="gkatre@mozilla.com",
        email=["gkatre@mozilla.com", "telemetry-alerts@mozilla.com"],
    )
