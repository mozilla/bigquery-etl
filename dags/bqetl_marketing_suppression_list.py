# Generated via https://github.com/mozilla/bigquery-etl/blob/main/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskMarker
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
import datetime
from operators.gcp_container_operator import GKEPodOperator
from utils.constants import ALLOWED_STATES, FAILED_STATES
from utils.gcp import bigquery_etl_query, bigquery_dq_check

docs = """
### bqetl_marketing_suppression_list

Built from bigquery-etl repo, [`dags/bqetl_marketing_suppression_list.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_marketing_suppression_list.py)

#### Description

Ingest marketing suppression lists into BigQuery

#### Owner

leli@mozilla.com

#### Tags

* impact/tier_2
* repo/bigquery-etl
"""


default_args = {
    "owner": "leli@mozilla.com",
    "start_date": datetime.datetime(2024, 4, 21, 0, 0),
    "end_date": None,
    "email": ["leli@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
}

tags = ["impact/tier_2", "repo/bigquery-etl"]

with DAG(
    "bqetl_marketing_suppression_list",
    default_args=default_args,
    schedule_interval="0 3 * * *",
    doc_md=docs,
    tags=tags,
) as dag:

    marketing_suppression_list_external__campaign_monitor_suppression_list__v1 = GKEPodOperator(
        task_id="marketing_suppression_list_external__campaign_monitor_suppression_list__v1",
        arguments=[
            "python",
            "sql/moz-fx-data-shared-prod/marketing_suppression_list_external/campaign_monitor_suppression_list_v1/query.py",
        ]
        + [
            "--api_key={{ var.value.campaign_monitor_api_key }}",
            "--client_id={{ var.value.campaign_monitor_client_id }}",
        ],
        image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
        owner="leli@mozilla.com",
        email=["leli@mozilla.com"],
    )
