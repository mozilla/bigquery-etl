# Generated via https://github.com/mozilla/bigquery-etl/blob/main/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskMarker
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
import datetime
from utils.constants import ALLOWED_STATES, FAILED_STATES
from utils.gcp import bigquery_etl_query, gke_command

docs = """
### bqetl_desktop_mobile_search_monthly

Built from bigquery-etl repo, [`dags/bqetl_desktop_mobile_search_monthly.py`](https://github.com/mozilla/bigquery-etl/blob/main/dags/bqetl_desktop_mobile_search_monthly.py)

#### Description

Generate mnthly client data from daily search table
#### Owner

akommasani@mozilla.com
"""


default_args = {
    "owner": "akommasani@mozilla.com",
    "start_date": datetime.datetime(2019, 1, 1, 0, 0),
    "end_date": None,
    "email": ["telemetry-alerts@mozilla.com", "akommasani@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
}

tags = ["impact/tier_1", "repo/bigquery-etl"]

with DAG(
    "bqetl_desktop_mobile_search_monthly",
    default_args=default_args,
    schedule_interval="0 5 2 * *",
    doc_md=docs,
    tags=tags,
) as dag:
    search_derived__desktop_mobile_search_clients_monthly__v1 = bigquery_etl_query(
        task_id="search_derived__desktop_mobile_search_clients_monthly__v1",
        destination_table='desktop_mobile_search_clients_monthly_v1${{ (execution_date + macros.dateutil.relativedelta.relativedelta(months=-1, day=1)).strftime("%Y%m") }}',
        dataset_id="search_derived",
        project_id="moz-fx-data-shared-prod",
        owner="akommasani@mozilla.com",
        email=["akommasani@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        parameters=["submission_date:DATE:{{ds}}"],
    )
