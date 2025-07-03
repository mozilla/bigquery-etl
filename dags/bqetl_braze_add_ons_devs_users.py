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
### bqetl_braze_add_ons_devs_users

Built from bigquery-etl repo, [`dags/bqetl_braze_add_ons_devs_users.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_braze_add_ons_devs_users.py)

#### Description

Load Add-ons Developers data into BigQuery for one-time send.

#### Owner

cbeck@mozilla.com

#### Tags

* impact/tier_3
* repo/bigquery-etl
"""


default_args = {
    "owner": "cbeck@mozilla.com",
    "start_date": datetime.datetime(2025, 7, 2, 0, 0),
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
    "bqetl_braze_add_ons_devs_users",
    default_args=default_args,
    schedule_interval="@once",
    doc_md=docs,
    tags=tags,
    catchup=False,
) as dag:

    braze_derived__add_ons_developers__v1 = GKEPodOperator(
        task_id="braze_derived__add_ons_developers__v1",
        arguments=[
            "python",
            "sql/moz-fx-data-shared-prod/braze_derived/add_ons_developers_v1/query.py",
        ]
        + [
            "--destination-project=moz-fx-data-shared-prod",
            "--destination-dataset=braze_derived",
            "--destination-table=add_ons_developers_v1",
            "--source-bucket=moz-fx-data-prod-external-pocket-data",
            "--source-prefix=braze_data_syncs",
            "--source-file=add_ons_devs",
        ],
        image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
        owner="cbeck@mozilla.com",
        email=["cbeck@mozilla.com"],
    )

    with TaskGroup(
        "braze_derived__add_ons_developers__v1_external",
    ) as braze_derived__add_ons_developers__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_braze_add_ons_sync__wait_for_braze_derived__add_ons_developers__v1",
            external_dag_id="bqetl_braze_add_ons_sync",
            external_task_id="wait_for_braze_derived__add_ons_developers__v1",
        )

        braze_derived__add_ons_developers__v1_external.set_upstream(
            braze_derived__add_ons_developers__v1
        )
