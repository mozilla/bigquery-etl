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
### bqetl_google_analytics_derived

Built from bigquery-etl repo, [`dags/bqetl_google_analytics_derived.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_google_analytics_derived.py)

#### Description

Daily aggregations of data exported from Google Analytics.

The GA export runs at 15:00 UTC, so there's an effective 2-day delay
for user activity to appear in these tables.

#### Owner

kwindau@mozilla.com

#### Tags

* impact/tier_1
* repo/bigquery-etl
"""


default_args = {
    "owner": "kwindau@mozilla.com",
    "start_date": datetime.datetime(2020, 10, 31, 0, 0),
    "end_date": None,
    "email": ["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
    "max_active_tis_per_dag": None,
}

tags = ["impact/tier_1", "repo/bigquery-etl"]

with DAG(
    "bqetl_google_analytics_derived",
    default_args=default_args,
    schedule_interval="0 23 * * *",
    doc_md=docs,
    tags=tags,
    catchup=False,
) as dag:

    mozilla_org_derived__blogs_daily_summary__v1 = bigquery_etl_query(
        task_id="mozilla_org_derived__blogs_daily_summary__v1",
        destination_table="blogs_daily_summary_v1",
        dataset_id="mozilla_org_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=[
            "ascholtz@mozilla.com",
            "kwindau@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    mozilla_org_derived__blogs_goals__v1 = bigquery_etl_query(
        task_id="mozilla_org_derived__blogs_goals__v1",
        destination_table="blogs_goals_v1",
        dataset_id="mozilla_org_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=[
            "ascholtz@mozilla.com",
            "kwindau@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    mozilla_org_derived__blogs_landing_page_summary__v1 = bigquery_etl_query(
        task_id="mozilla_org_derived__blogs_landing_page_summary__v1",
        destination_table="blogs_landing_page_summary_v1",
        dataset_id="mozilla_org_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=[
            "ascholtz@mozilla.com",
            "kwindau@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    mozilla_org_derived__blogs_sessions__v1 = bigquery_etl_query(
        task_id="mozilla_org_derived__blogs_sessions__v1",
        destination_table="blogs_sessions_v1",
        dataset_id="mozilla_org_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=[
            "ascholtz@mozilla.com",
            "kwindau@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    mozilla_org_derived__downloads_with_attribution__v1 = bigquery_etl_query(
        task_id="mozilla_org_derived__downloads_with_attribution__v1",
        destination_table="downloads_with_attribution_v1",
        dataset_id="mozilla_org_derived",
        project_id="moz-fx-data-shared-prod",
        owner="gleonard@mozilla.com",
        email=[
            "gleonard@mozilla.com",
            "kwindau@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    mozilla_org_derived__firefox_whatsnew_summary__v1 = bigquery_etl_query(
        task_id="mozilla_org_derived__firefox_whatsnew_summary__v1",
        destination_table="firefox_whatsnew_summary_v1",
        dataset_id="mozilla_org_derived",
        project_id="moz-fx-data-shared-prod",
        owner="rbaffourawuah@mozilla.com",
        email=[
            "kwindau@mozilla.com",
            "rbaffourawuah@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    mozilla_org_derived__www_site_downloads__v1 = bigquery_etl_query(
        task_id="mozilla_org_derived__www_site_downloads__v1",
        destination_table="www_site_downloads_v1",
        dataset_id="mozilla_org_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    mozilla_org_derived__www_site_events_metrics__v1 = bigquery_etl_query(
        task_id="mozilla_org_derived__www_site_events_metrics__v1",
        destination_table="www_site_events_metrics_v1",
        dataset_id="mozilla_org_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=[
            "ascholtz@mozilla.com",
            "kwindau@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    mozilla_org_derived__www_site_hits__v1 = bigquery_etl_query(
        task_id="mozilla_org_derived__www_site_hits__v1",
        destination_table="www_site_hits_v1",
        dataset_id="mozilla_org_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=[
            "ascholtz@mozilla.com",
            "kwindau@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    mozilla_org_derived__www_site_landing_page_metrics__v1 = bigquery_etl_query(
        task_id="mozilla_org_derived__www_site_landing_page_metrics__v1",
        destination_table="www_site_landing_page_metrics_v1",
        dataset_id="mozilla_org_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=[
            "ascholtz@mozilla.com",
            "kwindau@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    mozilla_org_derived__www_site_metrics_summary__v1 = bigquery_etl_query(
        task_id="mozilla_org_derived__www_site_metrics_summary__v1",
        destination_table="www_site_metrics_summary_v1",
        dataset_id="mozilla_org_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=[
            "ascholtz@mozilla.com",
            "kwindau@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    mozilla_org_derived__www_site_page_metrics__v1 = bigquery_etl_query(
        task_id="mozilla_org_derived__www_site_page_metrics__v1",
        destination_table="www_site_page_metrics_v1",
        dataset_id="mozilla_org_derived",
        project_id="moz-fx-data-shared-prod",
        owner="ascholtz@mozilla.com",
        email=[
            "ascholtz@mozilla.com",
            "kwindau@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    mozilla_org_derived__blogs_daily_summary__v1.set_upstream(
        mozilla_org_derived__blogs_goals__v1
    )

    mozilla_org_derived__blogs_daily_summary__v1.set_upstream(
        mozilla_org_derived__blogs_sessions__v1
    )

    mozilla_org_derived__blogs_landing_page_summary__v1.set_upstream(
        mozilla_org_derived__blogs_goals__v1
    )

    mozilla_org_derived__blogs_landing_page_summary__v1.set_upstream(
        mozilla_org_derived__blogs_sessions__v1
    )

    mozilla_org_derived__firefox_whatsnew_summary__v1.set_upstream(
        mozilla_org_derived__www_site_hits__v1
    )

    mozilla_org_derived__www_site_downloads__v1.set_upstream(
        mozilla_org_derived__www_site_hits__v1
    )

    mozilla_org_derived__www_site_events_metrics__v1.set_upstream(
        mozilla_org_derived__www_site_hits__v1
    )

    mozilla_org_derived__www_site_landing_page_metrics__v1.set_upstream(
        mozilla_org_derived__www_site_downloads__v1
    )

    mozilla_org_derived__www_site_landing_page_metrics__v1.set_upstream(
        mozilla_org_derived__www_site_hits__v1
    )

    mozilla_org_derived__www_site_metrics_summary__v1.set_upstream(
        mozilla_org_derived__www_site_downloads__v1
    )

    mozilla_org_derived__www_site_page_metrics__v1.set_upstream(
        mozilla_org_derived__www_site_hits__v1
    )
