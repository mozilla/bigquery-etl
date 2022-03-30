# Generated via https://github.com/mozilla/bigquery-etl/blob/main/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from operators.task_sensor import ExternalTaskCompletedSensor
import datetime
from utils.gcp import bigquery_etl_query, gke_command

docs = """
### bqetl_cjms_nonprod

Built from bigquery-etl repo, [`dags/bqetl_cjms_nonprod.py`](https://github.com/mozilla/bigquery-etl/blob/main/dags/bqetl_cjms_nonprod.py)

#### Description

Hourly ETL for cjms nonprod.

Scheduled to run after fivetran stripe_nonprod, which runs hourly at minute
17 and takes less than 5 minutes.

#### Owner

dthorn@mozilla.com
"""


default_args = {
    "owner": "dthorn@mozilla.com",
    "start_date": datetime.datetime(2022, 3, 24, 0, 0),
    "end_date": None,
    "email": ["telemetry-alerts@mozilla.com", "dthorn@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=300),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
}

tags = ["impact/tier_3", "repo/bigquery-etl"]

with DAG(
    "bqetl_cjms_nonprod",
    default_args=default_args,
    schedule_interval="22 * * * *",
    doc_md=docs,
    tags=tags,
) as dag:

    cjms_bigquery__flows__v1 = bigquery_etl_query(
        task_id="cjms_bigquery__flows__v1",
        destination_table="flows_v1",
        dataset_id="moz-fx-cjms-nonprod-9a36:cjms_bigquery",
        project_id="moz-fx-data-shared-prod",
        sql_file_path="sql/moz-fx-cjms-nonprod-9a36/cjms_bigquery/flows_v1/query.sql",
        owner="dthorn@mozilla.com",
        email=["dthorn@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        dag=dag,
    )

    cjms_bigquery__refunds__v1 = bigquery_etl_query(
        task_id="cjms_bigquery__refunds__v1",
        destination_table="refunds_v1",
        dataset_id="moz-fx-cjms-nonprod-9a36:cjms_bigquery",
        project_id="moz-fx-data-shared-prod",
        sql_file_path="sql/moz-fx-cjms-nonprod-9a36/cjms_bigquery/refunds_v1/query.sql",
        owner="dthorn@mozilla.com",
        email=["dthorn@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        dag=dag,
    )

    cjms_bigquery__subscriptions__v1 = bigquery_etl_query(
        task_id="cjms_bigquery__subscriptions__v1",
        destination_table="subscriptions_v1",
        dataset_id="moz-fx-cjms-nonprod-9a36:cjms_bigquery",
        project_id="moz-fx-data-shared-prod",
        sql_file_path="sql/moz-fx-cjms-nonprod-9a36/cjms_bigquery/subscriptions_v1/query.sql",
        owner="dthorn@mozilla.com",
        email=["dthorn@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        dag=dag,
    )

    cjms_bigquery__subscriptions__v1.set_upstream(cjms_bigquery__flows__v1)
