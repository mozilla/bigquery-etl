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
### bqetl_newtab_historical

Built from bigquery-etl repo, [`dags/bqetl_newtab_historical.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_newtab_historical.py)

#### Description

Load Newtab historical data from GCS into BigQuery

#### Owner

cbeck@mozilla.com

#### Tags

* impact/tier_3
* repo/bigquery-etl
"""


default_args = {
    "owner": "cbeck@mozilla.com",
    "start_date": datetime.datetime(2025, 5, 13, 0, 0),
    "end_date": None,
    "email": ["cbeck@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=300),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
    "max_active_tis_per_dag": None,
}

tags = ["impact/tier_3", "repo/bigquery-etl"]

with DAG(
    "bqetl_newtab_historical",
    default_args=default_args,
    schedule_interval="0 2 * * *",
    doc_md=docs,
    tags=tags,
    catchup=False,
) as dag:

    telemetry_derived__newtab_interactions_historical_legacy__v1 = GKEPodOperator(
        task_id="telemetry_derived__newtab_interactions_historical_legacy__v1",
        arguments=[
            "python",
            "sql/moz-fx-data-shared-prod/telemetry_derived/newtab_interactions_historical_legacy_v1/query.py",
        ]
        + [
            "--destination-project=moz-fx-data-shared-prod",
            "--destination-dataset=telemetry_derived",
            "--destination-table=newtab_interactions_historical_legacy_v1",
            "--source-bucket=moz-fx-data-prod-external-pocket-data",
            "--source-prefix=newtab_interactions_historical",
            "--source-file=firefox_newtab_legacy_interactions",
        ],
        image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
        owner="cbeck@mozilla.com",
        email=["cbeck@mozilla.com"],
    )

    with TaskGroup(
        "telemetry_derived__newtab_interactions_historical_legacy__v1_external",
    ) as telemetry_derived__newtab_interactions_historical_legacy__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_newtab_interactions_hourly__wait_for_telemetry_derived__newtab_interactions_historical_legacy__v1",
            external_dag_id="bqetl_newtab_interactions_hourly",
            external_task_id="wait_for_telemetry_derived__newtab_interactions_historical_legacy__v1",
            execution_date="{{ (execution_date - macros.timedelta(seconds=7200)).isoformat() }}",
        )

        telemetry_derived__newtab_interactions_historical_legacy__v1_external.set_upstream(
            telemetry_derived__newtab_interactions_historical_legacy__v1
        )
