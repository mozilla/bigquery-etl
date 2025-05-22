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
### bqetl_content_ml_daily

Built from bigquery-etl repo, [`dags/bqetl_content_ml_daily.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_content_ml_daily.py)

#### Description

Daily extracts for corpus item data to be evaluated for new tab presentation.

#### Owner

skamath@mozilla.com

#### Tags

* impact/tier_2
* repo/bigquery-etl
"""


default_args = {
    "owner": "skamath@mozilla.com",
    "start_date": datetime.datetime(2025, 5, 5, 0, 0),
    "end_date": None,
    "email": ["skamath@mozilla.com", "mlcooper@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
    "max_active_tis_per_dag": None,
}

tags = ["impact/tier_2", "repo/bigquery-etl"]

with DAG(
    "bqetl_content_ml_daily",
    default_args=default_args,
    schedule_interval="0 4 * * *",
    doc_md=docs,
    tags=tags,
    catchup=False,
) as dag:

    snowflake_migration__prospects__v1 = bigquery_etl_query(
        task_id="snowflake_migration__prospects__v1",
        destination_table="prospects_v1",
        dataset_id="snowflake_migration",
        project_id="moz-fx-data-shared-prod",
        owner="skamath@mozilla.com",
        email=["mlcooper@mozilla.com", "skamath@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )
