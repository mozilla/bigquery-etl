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
### bqetl_google_analytics_derived_ga4

Built from bigquery-etl repo, [`dags/bqetl_google_analytics_derived_ga4.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_google_analytics_derived_ga4.py)

#### Description

Daily aggregations of data exported from Google Analytics 4
#### Owner

kwindau@mozilla.com

#### Tags

* impact/tier_2
* repo/bigquery-etl
"""


default_args = {
    "owner": "kwindau@mozilla.com",
    "start_date": datetime.datetime(2024, 1, 3, 0, 0),
    "end_date": None,
    "email": ["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
}

tags = ["impact/tier_2", "repo/bigquery-etl"]

with DAG(
    "bqetl_google_analytics_derived_ga4",
    default_args=default_args,
    schedule_interval="0 12 * * *",
    doc_md=docs,
    tags=tags,
) as dag:
    ga_derived__blogs_sessions__v2 = bigquery_etl_query(
        task_id="ga_derived__blogs_sessions__v2",
        destination_table="blogs_sessions_v2",
        dataset_id="ga_derived",
        project_id="moz-fx-data-marketing-prod",
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    ga_derived__firefox_whatsnew_summary__v2 = bigquery_etl_query(
        task_id="ga_derived__firefox_whatsnew_summary__v2",
        destination_table="firefox_whatsnew_summary_v2",
        dataset_id="ga_derived",
        project_id="moz-fx-data-marketing-prod",
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    ga_derived__www_site_downloads__v2 = bigquery_etl_query(
        task_id="ga_derived__www_site_downloads__v2",
        destination_table="www_site_downloads_v2",
        dataset_id="ga_derived",
        project_id="moz-fx-data-marketing-prod",
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    ga_derived__www_site_events_metrics__v2 = bigquery_etl_query(
        task_id="ga_derived__www_site_events_metrics__v2",
        destination_table="www_site_events_metrics_v2",
        dataset_id="ga_derived",
        project_id="moz-fx-data-marketing-prod",
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    ga_derived__www_site_hits__v2 = bigquery_etl_query(
        task_id="ga_derived__www_site_hits__v2",
        destination_table="www_site_hits_v2",
        dataset_id="ga_derived",
        project_id="moz-fx-data-marketing-prod",
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    ga_derived__www_site_landing_page_metrics__v2 = bigquery_etl_query(
        task_id="ga_derived__www_site_landing_page_metrics__v2",
        destination_table="www_site_landing_page_metrics_v2",
        dataset_id="ga_derived",
        project_id="moz-fx-data-marketing-prod",
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    ga_derived__www_site_metrics_summary__v2 = bigquery_etl_query(
        task_id="ga_derived__www_site_metrics_summary__v2",
        destination_table="www_site_metrics_summary_v2",
        dataset_id="ga_derived",
        project_id="moz-fx-data-marketing-prod",
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    ga_derived__www_site_page_metrics__v2 = bigquery_etl_query(
        task_id="ga_derived__www_site_page_metrics__v2",
        destination_table="www_site_page_metrics_v2",
        dataset_id="ga_derived",
        project_id="moz-fx-data-marketing-prod",
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    mozilla_org_derived__ga_sessions__v2 = bigquery_etl_query(
        task_id="mozilla_org_derived__ga_sessions__v2",
        destination_table=None,
        dataset_id="mozilla_org_derived",
        project_id="moz-fx-data-shared-prod",
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        sql_file_path="sql/moz-fx-data-shared-prod/mozilla_org_derived/ga_sessions_v2/script.sql",
    )

    with TaskGroup(
        "mozilla_org_derived__ga_sessions__v2_external",
    ) as mozilla_org_derived__ga_sessions__v2_external:
        ExternalTaskMarker(
            task_id="bqetl_mozilla_org_derived__wait_for_mozilla_org_derived__ga_sessions__v2",
            external_dag_id="bqetl_mozilla_org_derived",
            external_task_id="wait_for_mozilla_org_derived__ga_sessions__v2",
            execution_date="{{ (execution_date - macros.timedelta(seconds=36000)).isoformat() }}",
        )

        mozilla_org_derived__ga_sessions__v2_external.set_upstream(
            mozilla_org_derived__ga_sessions__v2
        )

    ga_derived__firefox_whatsnew_summary__v2.set_upstream(ga_derived__www_site_hits__v2)

    ga_derived__www_site_events_metrics__v2.set_upstream(ga_derived__www_site_hits__v2)

    ga_derived__www_site_landing_page_metrics__v2.set_upstream(
        ga_derived__www_site_hits__v2
    )

    ga_derived__www_site_page_metrics__v2.set_upstream(ga_derived__www_site_hits__v2)
