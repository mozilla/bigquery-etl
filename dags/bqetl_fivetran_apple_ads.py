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
### bqetl_fivetran_apple_ads

Built from bigquery-etl repo, [`dags/bqetl_fivetran_apple_ads.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_fivetran_apple_ads.py)

#### Description

Copies over apple_ads data coming from Fivetran
into our data BQ project. Fivetran syncs this data
every hour. We copy the data every 3 hours to our project.

#### Owner

kik@mozilla.com

#### Tags

* impact/tier_2
* repo/bigquery-etl
"""


default_args = {
    "owner": "kik@mozilla.com",
    "start_date": datetime.datetime(2023, 5, 25, 0, 0),
    "end_date": None,
    "email": ["telemetry-alerts@mozilla.com", "frank@mozilla.com", "kik@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
    "max_active_tis_per_dag": None,
}

tags = ["impact/tier_2", "repo/bigquery-etl"]

with DAG(
    "bqetl_fivetran_apple_ads",
    default_args=default_args,
    schedule_interval="0 3 * * *",
    doc_md=docs,
    tags=tags,
    catchup=False,
) as dag:

    apple_ads_external__ad_group_report__v1 = bigquery_etl_query(
        task_id="apple_ads_external__ad_group_report__v1",
        destination_table="ad_group_report_v1",
        dataset_id="apple_ads_external",
        project_id="moz-fx-data-shared-prod",
        owner="kik@mozilla.com",
        email=["frank@mozilla.com", "kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    with TaskGroup(
        "apple_ads_external__ad_group_report__v1_external",
    ) as apple_ads_external__ad_group_report__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_ios_campaign_reporting__wait_for_apple_ads_external__ad_group_report__v1",
            external_dag_id="bqetl_ios_campaign_reporting",
            external_task_id="wait_for_apple_ads_external__ad_group_report__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=54000)).isoformat() }}",
        )

        apple_ads_external__ad_group_report__v1_external.set_upstream(
            apple_ads_external__ad_group_report__v1
        )

    apple_ads_external__campaign_report__v1 = bigquery_etl_query(
        task_id="apple_ads_external__campaign_report__v1",
        destination_table="campaign_report_v1",
        dataset_id="apple_ads_external",
        project_id="moz-fx-data-shared-prod",
        owner="kik@mozilla.com",
        email=["frank@mozilla.com", "kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    apple_ads_external__keyword_report__v1 = bigquery_etl_query(
        task_id="apple_ads_external__keyword_report__v1",
        destination_table="keyword_report_v1",
        dataset_id="apple_ads_external",
        project_id="moz-fx-data-shared-prod",
        owner="kik@mozilla.com",
        email=["frank@mozilla.com", "kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    apple_ads_external__organization_report__v1 = bigquery_etl_query(
        task_id="apple_ads_external__organization_report__v1",
        destination_table="organization_report_v1",
        dataset_id="apple_ads_external",
        project_id="moz-fx-data-shared-prod",
        owner="kik@mozilla.com",
        email=["frank@mozilla.com", "kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    apple_ads_external__search_term_report__v1 = bigquery_etl_query(
        task_id="apple_ads_external__search_term_report__v1",
        destination_table="search_term_report_v1",
        dataset_id="apple_ads_external",
        project_id="moz-fx-data-shared-prod",
        owner="kik@mozilla.com",
        email=["frank@mozilla.com", "kik@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )
