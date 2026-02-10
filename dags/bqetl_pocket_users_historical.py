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
### bqetl_pocket_users_historical

Built from bigquery-etl repo, [`dags/bqetl_pocket_users_historical.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_pocket_users_historical.py)

#### Description

Load Pocket Users historical data from GCS into BigQuery

#### Owner

lmcfall@mozilla.com

#### Tags

* impact/tier_3
* repo/bigquery-etl
"""


default_args = {
    "owner": "lmcfall@mozilla.com",
    "start_date": datetime.datetime(2025, 6, 12, 0, 0),
    "end_date": None,
    "email": ["lmcfall@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=300),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
    "max_active_tis_per_dag": None,
}

tags = ["impact/tier_3", "repo/bigquery-etl"]

with DAG(
    "bqetl_pocket_users_historical",
    default_args=default_args,
    schedule_interval="@once",
    doc_md=docs,
    tags=tags,
    catchup=False,
) as dag:

    braze_external__pocket_users_historical__v1 = GKEPodOperator(
        task_id="braze_external__pocket_users_historical__v1",
        arguments=[
            "python",
            "sql/moz-fx-data-shared-prod/braze_external/pocket_users_historical_v1/query.py",
        ]
        + [
            "--destination-project=moz-fx-data-shared-prod",
            "--destination-dataset=braze_external",
            "--destination-table=pocket_users_historical_v1",
            "--source-bucket=moz-fx-data-prod-external-pocket-data",
            "--source-prefix=braze_pocket_historical",
            "--source-file=pocket_users_historical",
        ],
        image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
        owner="lmcfall@mozilla.com",
        email=["lmcfall@mozilla.com"],
    )
