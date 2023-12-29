# Generated via https://github.com/mozilla/bigquery-etl/blob/main/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskMarker
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
import datetime
from utils.constants import ALLOWED_STATES, FAILED_STATES
from utils.gcp import bigquery_etl_query, gke_command, bigquery_dq_check

docs = """
### bqetl_google_analytics_derived

Built from bigquery-etl repo, [`dags/bqetl_google_analytics_derived.py`](https://github.com/mozilla/bigquery-etl/blob/main/dags/bqetl_google_analytics_derived.py)

#### Description

Daily aggregations of data exported from Google Analytics.

The GA export runs at 15:00 UTC, so there's an effective 2-day delay
for user activity to appear in these tables.

#### Owner

ascholtz@mozilla.com
"""


default_args = {
    "owner": "ascholtz@mozilla.com",
    "start_date": datetime.datetime(2020, 10, 31, 0, 0),
    "end_date": None,
    "email": ["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
}

tags = ["impact/tier_1", "repo/bigquery-etl"]

with DAG(
    "bqetl_google_analytics_derived",
    default_args=default_args,
    schedule_interval="0 23 * * *",
    doc_md=docs,
    tags=tags,
) as dag:
    ga_derived__blogs_daily_summary__v1 = bigquery_etl_query(
        task_id="ga_derived__blogs_daily_summary__v1",
        destination_table="blogs_daily_summary_v1",
        dataset_id="ga_derived",
        project_id="moz-fx-data-marketing-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    ga_derived__blogs_empty_check__v1 = bigquery_etl_query(
        task_id="ga_derived__blogs_empty_check__v1",
        destination_table=None,
        dataset_id="ga_derived",
        project_id="moz-fx-data-marketing-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        parameters=["submission_date:DATE:{{ds}}"],
        sql_file_path="sql/moz-fx-data-marketing-prod/ga_derived/blogs_empty_check_v1/query.sql",
    )

    ga_derived__blogs_goals__v1 = bigquery_etl_query(
        task_id="ga_derived__blogs_goals__v1",
        destination_table="blogs_goals_v1",
        dataset_id="ga_derived",
        project_id="moz-fx-data-marketing-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    ga_derived__blogs_landing_page_summary__v1 = bigquery_etl_query(
        task_id="ga_derived__blogs_landing_page_summary__v1",
        destination_table="blogs_landing_page_summary_v1",
        dataset_id="ga_derived",
        project_id="moz-fx-data-marketing-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    ga_derived__blogs_sessions__v1 = bigquery_etl_query(
        task_id="ga_derived__blogs_sessions__v1",
        destination_table="blogs_sessions_v1",
        dataset_id="ga_derived",
        project_id="moz-fx-data-marketing-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    ga_derived__downloads_with_attribution__v1 = bigquery_etl_query(
        task_id="ga_derived__downloads_with_attribution__v1",
        destination_table="downloads_with_attribution_v1",
        dataset_id="ga_derived",
        project_id="moz-fx-data-marketing-prod",
        owner="gleonard@mozilla.com",
        email=[
            "ascholtz@mozilla.com",
            "gleonard@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    with TaskGroup(
        "ga_derived__downloads_with_attribution__v1_external",
    ) as ga_derived__downloads_with_attribution__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_desktop_installs_v1__wait_for_ga_derived__downloads_with_attribution__v1",
            external_dag_id="bqetl_desktop_installs_v1",
            external_task_id="wait_for_ga_derived__downloads_with_attribution__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=83100)).isoformat() }}",
        )

        ga_derived__downloads_with_attribution__v1_external.set_upstream(
            ga_derived__downloads_with_attribution__v1
        )

    ga_derived__firefox_whatsnew_summary__v1 = bigquery_etl_query(
        task_id="ga_derived__firefox_whatsnew_summary__v1",
        destination_table="firefox_whatsnew_summary_v1",
        dataset_id="ga_derived",
        project_id="moz-fx-data-marketing-prod",
        owner="rbaffourawuah@mozilla.com",
        email=[
            "ascholtz@mozilla.com",
            "rbaffourawuah@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    ga_derived__www_site_downloads__v1 = bigquery_etl_query(
        task_id="ga_derived__www_site_downloads__v1",
        destination_table="www_site_downloads_v1",
        dataset_id="ga_derived",
        project_id="moz-fx-data-marketing-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    ga_derived__www_site_empty_check__v1 = bigquery_etl_query(
        task_id="ga_derived__www_site_empty_check__v1",
        destination_table=None,
        dataset_id="ga_derived",
        project_id="moz-fx-data-marketing-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        parameters=["submission_date:DATE:{{ds}}"],
        sql_file_path="sql/moz-fx-data-marketing-prod/ga_derived/www_site_empty_check_v1/query.sql",
    )

    with TaskGroup(
        "ga_derived__www_site_empty_check__v1_external",
    ) as ga_derived__www_site_empty_check__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_download_funnel_attribution__wait_for_ga_derived__www_site_empty_check__v1",
            external_dag_id="bqetl_download_funnel_attribution",
            external_task_id="wait_for_ga_derived__www_site_empty_check__v1",
        )

        ga_derived__www_site_empty_check__v1_external.set_upstream(
            ga_derived__www_site_empty_check__v1
        )

    ga_derived__www_site_events_metrics__v1 = bigquery_etl_query(
        task_id="ga_derived__www_site_events_metrics__v1",
        destination_table="www_site_events_metrics_v1",
        dataset_id="ga_derived",
        project_id="moz-fx-data-marketing-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    ga_derived__www_site_hits__v1 = bigquery_etl_query(
        task_id="ga_derived__www_site_hits__v1",
        destination_table="www_site_hits_v1",
        dataset_id="ga_derived",
        project_id="moz-fx-data-marketing-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    ga_derived__www_site_landing_page_metrics__v1 = bigquery_etl_query(
        task_id="ga_derived__www_site_landing_page_metrics__v1",
        destination_table="www_site_landing_page_metrics_v1",
        dataset_id="ga_derived",
        project_id="moz-fx-data-marketing-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    ga_derived__www_site_metrics_summary__v1 = bigquery_etl_query(
        task_id="ga_derived__www_site_metrics_summary__v1",
        destination_table="www_site_metrics_summary_v1",
        dataset_id="ga_derived",
        project_id="moz-fx-data-marketing-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    ga_derived__www_site_page_metrics__v1 = bigquery_etl_query(
        task_id="ga_derived__www_site_page_metrics__v1",
        destination_table="www_site_page_metrics_v1",
        dataset_id="ga_derived",
        project_id="moz-fx-data-marketing-prod",
        owner="ascholtz@mozilla.com",
        email=["ascholtz@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    ga_derived__blogs_daily_summary__v1.set_upstream(ga_derived__blogs_goals__v1)

    ga_derived__blogs_daily_summary__v1.set_upstream(ga_derived__blogs_sessions__v1)

    ga_derived__blogs_goals__v1.set_upstream(ga_derived__blogs_empty_check__v1)

    ga_derived__blogs_landing_page_summary__v1.set_upstream(ga_derived__blogs_goals__v1)

    ga_derived__blogs_landing_page_summary__v1.set_upstream(
        ga_derived__blogs_sessions__v1
    )

    ga_derived__blogs_sessions__v1.set_upstream(ga_derived__blogs_empty_check__v1)

    ga_derived__downloads_with_attribution__v1.set_upstream(
        ga_derived__www_site_empty_check__v1
    )

    ga_derived__firefox_whatsnew_summary__v1.set_upstream(ga_derived__www_site_hits__v1)

    ga_derived__www_site_downloads__v1.set_upstream(ga_derived__www_site_hits__v1)

    ga_derived__www_site_events_metrics__v1.set_upstream(ga_derived__www_site_hits__v1)

    ga_derived__www_site_hits__v1.set_upstream(ga_derived__www_site_empty_check__v1)

    ga_derived__www_site_landing_page_metrics__v1.set_upstream(
        ga_derived__www_site_downloads__v1
    )

    ga_derived__www_site_landing_page_metrics__v1.set_upstream(
        ga_derived__www_site_hits__v1
    )

    ga_derived__www_site_metrics_summary__v1.set_upstream(
        ga_derived__www_site_downloads__v1
    )

    ga_derived__www_site_page_metrics__v1.set_upstream(ga_derived__www_site_hits__v1)
