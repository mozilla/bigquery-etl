# Generated via https://github.com/mozilla/bigquery-etl/blob/master/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from airflow.operators.sensors import ExternalTaskSensor
import datetime
from utils.gcp import bigquery_etl_query, gke_command

docs = """
### bqetl_marketing_fetch

Built from bigquery-etl repo, [`dags/bqetl_marketing_fetch.py`](https://github.com/mozilla/bigquery-etl/blob/master/dags/bqetl_marketing_fetch.py)

#### Owner

bewu@mozilla.com
"""


default_args = {
    "owner": "bewu@mozilla.com",
    "start_date": datetime.datetime(2020, 11, 30, 0, 0),
    "end_date": None,
    "email": ["bewu@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
}

with DAG(
    "bqetl_marketing_fetch",
    default_args=default_args,
    schedule_interval="0 1 * * 1",
    doc_md=docs,
) as dag:

    fetch__spend_alignment_by_campaign__v1 = bigquery_etl_query(
        task_id="fetch__spend_alignment_by_campaign__v1",
        destination_table="spend_alignment_by_campaign_v1",
        dataset_id="fetch",
        project_id="moz-fx-data-marketing-prod",
        owner="bewu@mozilla.com",
        email=["bewu@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        dag=dag,
    )

    fetch__spend_alignment_by_month__v1 = bigquery_etl_query(
        task_id="fetch__spend_alignment_by_month__v1",
        destination_table="spend_alignment_by_month_v1",
        dataset_id="fetch",
        project_id="moz-fx-data-marketing-prod",
        owner="bewu@mozilla.com",
        email=["bewu@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        dag=dag,
    )
