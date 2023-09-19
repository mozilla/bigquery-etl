# Generated via https://github.com/mozilla/bigquery-etl/blob/main/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskMarker
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
import datetime
from utils.constants import ALLOWED_STATES, FAILED_STATES
from utils.gcp import bigquery_etl_query, gke_command, bigquery_dq_check

docs = """
### bqetl_campaign_cost_breakdowns

Built from bigquery-etl repo, [`dags/bqetl_campaign_cost_breakdowns.py`](https://github.com/mozilla/bigquery-etl/blob/main/dags/bqetl_campaign_cost_breakdowns.py)

#### Description

Derived tables on top of fenix installation and DOU metrics,
as well as Google ads campaign data.

#### Owner

ctroy@mozilla.com
"""


default_args = {
    "owner": "ctroy@mozilla.com",
    "start_date": datetime.datetime(2021, 9, 20, 0, 0),
    "end_date": None,
    "email": ["ctroy@mozilla.com", "frank@mozilla.com", "telemetry-alerts@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
}

tags = ["impact/tier_2", "repo/bigquery-etl"]

with DAG(
    "bqetl_campaign_cost_breakdowns",
    default_args=default_args,
    schedule_interval="0 3 * * *",
    doc_md=docs,
    tags=tags,
) as dag:
    fenix_derived__google_ads_campaign_cost_breakdowns__v1 = bigquery_etl_query(
        task_id="fenix_derived__google_ads_campaign_cost_breakdowns__v1",
        destination_table="google_ads_campaign_cost_breakdowns_v1",
        dataset_id="fenix_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ctroy@mozilla.com",
        email=[
            "ctroy@mozilla.com",
            "frank@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    wait_for_fenix_derived__attributable_clients__v1 = ExternalTaskSensor(
        task_id="wait_for_fenix_derived__attributable_clients__v1",
        external_dag_id="bqetl_org_mozilla_firefox_derived",
        external_task_id="fenix_derived__attributable_clients__v1",
        execution_delta=datetime.timedelta(seconds=3600),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    fenix_derived__google_ads_campaign_cost_breakdowns__v1.set_upstream(
        wait_for_fenix_derived__attributable_clients__v1
    )
