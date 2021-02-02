# Generated via https://github.com/mozilla/bigquery-etl/blob/master/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from airflow.operators.sensors import ExternalTaskSensor
import datetime
from utils.gcp import bigquery_etl_query, gke_command

docs = """
### bqetl_google_analytics_derived

Built from bigquery-etl repo, [`dags/bqetl_google_analytics_derived.py`](https://github.com/mozilla/bigquery-etl/blob/master/dags/bqetl_google_analytics_derived.py)

#### Owner

bewu@mozilla.com
"""


default_args = {
    "owner": "bewu@mozilla.com",
    "start_date": datetime.datetime(2020, 10, 31, 0, 0),
    "end_date": None,
    "email": ["bewu@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
}

with DAG(
    "bqetl_google_analytics_derived",
    default_args=default_args,
    schedule_interval="0 23 * * *",
    doc_md=docs,
) as dag:

    ga_derived__blogs_daily_summary__v1 = bigquery_etl_query(
        task_id="ga_derived__blogs_daily_summary__v1",
        destination_table="blogs_daily_summary_v1",
        dataset_id="ga_derived",
        project_id="moz-fx-data-marketing-prod",
        owner="bewu@mozilla.com",
        email=["bewu@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        dag=dag,
    )

    ga_derived__blogs_empty_check__v1 = bigquery_etl_query(
        task_id="ga_derived__blogs_empty_check__v1",
        destination_table=None,
        dataset_id="ga_derived",
        project_id="moz-fx-data-marketing-prod",
        owner="bewu@mozilla.com",
        email=["bewu@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        parameters=["submission_date:DATE:{{ds}}"],
        sql_file_path="sql/moz-fx-data-marketing-prod/ga_derived/blogs_empty_check_v1/query.sql",
        dag=dag,
    )

    ga_derived__blogs_goals__v1 = bigquery_etl_query(
        task_id="ga_derived__blogs_goals__v1",
        destination_table="blogs_goals_v1",
        dataset_id="ga_derived",
        project_id="moz-fx-data-marketing-prod",
        owner="bewu@mozilla.com",
        email=["bewu@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        dag=dag,
    )

    ga_derived__blogs_landing_page_summary__v1 = bigquery_etl_query(
        task_id="ga_derived__blogs_landing_page_summary__v1",
        destination_table="blogs_landing_page_summary_v1",
        dataset_id="ga_derived",
        project_id="moz-fx-data-marketing-prod",
        owner="bewu@mozilla.com",
        email=["bewu@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        dag=dag,
    )

    ga_derived__blogs_sessions__v1 = bigquery_etl_query(
        task_id="ga_derived__blogs_sessions__v1",
        destination_table="blogs_sessions_v1",
        dataset_id="ga_derived",
        project_id="moz-fx-data-marketing-prod",
        owner="bewu@mozilla.com",
        email=["bewu@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        dag=dag,
    )

    ga_derived__www_site_downloads__v1 = bigquery_etl_query(
        task_id="ga_derived__www_site_downloads__v1",
        destination_table="www_site_downloads_v1",
        dataset_id="ga_derived",
        project_id="moz-fx-data-marketing-prod",
        owner="bewu@mozilla.com",
        email=["bewu@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        dag=dag,
    )

    ga_derived__www_site_empty_check__v1 = bigquery_etl_query(
        task_id="ga_derived__www_site_empty_check__v1",
        destination_table=None,
        dataset_id="ga_derived",
        project_id="moz-fx-data-marketing-prod",
        owner="bewu@mozilla.com",
        email=["bewu@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        parameters=["submission_date:DATE:{{ds}}"],
        sql_file_path="sql/moz-fx-data-marketing-prod/ga_derived/www_site_empty_check_v1/query.sql",
        dag=dag,
    )

    ga_derived__www_site_events_metrics__v1 = bigquery_etl_query(
        task_id="ga_derived__www_site_events_metrics__v1",
        destination_table="www_site_events_metrics_v1",
        dataset_id="ga_derived",
        project_id="moz-fx-data-marketing-prod",
        owner="bewu@mozilla.com",
        email=["bewu@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        dag=dag,
    )

    ga_derived__www_site_hits__v1 = bigquery_etl_query(
        task_id="ga_derived__www_site_hits__v1",
        destination_table="www_site_hits_v1",
        dataset_id="ga_derived",
        project_id="moz-fx-data-marketing-prod",
        owner="bewu@mozilla.com",
        email=["bewu@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        dag=dag,
    )

    ga_derived__www_site_landing_page_metrics__v1 = bigquery_etl_query(
        task_id="ga_derived__www_site_landing_page_metrics__v1",
        destination_table="www_site_landing_page_metrics_v1",
        dataset_id="ga_derived",
        project_id="moz-fx-data-marketing-prod",
        owner="bewu@mozilla.com",
        email=["bewu@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        dag=dag,
    )

    ga_derived__www_site_metrics_summary__v1 = bigquery_etl_query(
        task_id="ga_derived__www_site_metrics_summary__v1",
        destination_table="www_site_metrics_summary_v1",
        dataset_id="ga_derived",
        project_id="moz-fx-data-marketing-prod",
        owner="bewu@mozilla.com",
        email=["bewu@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        dag=dag,
    )

    ga_derived__www_site_page_metrics__v1 = bigquery_etl_query(
        task_id="ga_derived__www_site_page_metrics__v1",
        destination_table="www_site_page_metrics_v1",
        dataset_id="ga_derived",
        project_id="moz-fx-data-marketing-prod",
        owner="bewu@mozilla.com",
        email=["bewu@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        dag=dag,
    )

    ga_derived__blogs_daily_summary__v1.set_upstream(ga_derived__blogs_goals__v1)

    ga_derived__blogs_daily_summary__v1.set_upstream(ga_derived__blogs_sessions__v1)

    ga_derived__blogs_goals__v1.set_upstream(ga_derived__blogs_empty_check__v1)

    ga_derived__blogs_landing_page_summary__v1.set_upstream(ga_derived__blogs_goals__v1)

    ga_derived__blogs_landing_page_summary__v1.set_upstream(
        ga_derived__blogs_sessions__v1
    )

    ga_derived__blogs_sessions__v1.set_upstream(ga_derived__blogs_empty_check__v1)

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
