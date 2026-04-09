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
### bqetl_sumo_metrics

Built from bigquery-etl repo, [`dags/bqetl_sumo_metrics.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_sumo_metrics.py)

#### Description

Daily derived metrics for Mozilla Support (SUMO) covering:
- Content freshness and quality tracking for knowledge base articles
- Translation/localization metrics for priority locales (30-day SLA)
- Forum questions and engagement metrics from Kitsune
- Support ticket volume from Zendesk

#### Owner

phlee@mozilla.com

#### Tags

* impact/tier_2
* repo/bigquery-etl
"""


default_args = {
    "owner": "phlee@mozilla.com",
    "start_date": datetime.datetime(2025, 7, 14, 0, 0),
    "end_date": None,
    "email": ["phlee@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "max_active_tis_per_dag": None,
}

tags = ["impact/tier_2", "repo/bigquery-etl"]

with DAG(
    "bqetl_sumo_metrics",
    default_args=default_args,
    schedule_interval="0 3 * * *",
    doc_md=docs,
    tags=tags,
    catchup=False,
) as dag:

    wait_for_sumo_ga_derived__ga4_events__v1 = ExternalTaskSensor(
        task_id="wait_for_sumo_ga_derived__ga4_events__v1",
        external_dag_id="bqetl_google_analytics_derived_ga4",
        external_task_id="sumo_ga_derived__ga4_events__v1",
        execution_delta=datetime.timedelta(days=-1, seconds=54000),
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    sumo_metrics_derived__freshness_metrics_base__v1 = bigquery_etl_query(
        task_id="sumo_metrics_derived__freshness_metrics_base__v1",
        destination_table="freshness_metrics_base_v1",
        dataset_id="sumo_metrics_derived",
        project_id="moz-fx-data-shared-prod",
        owner="plee@mozilla.com",
        email=["phlee@mozilla.com", "plee@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    sumo_metrics_derived__ga4_engagement_sessions_daily__v1 = bigquery_etl_query(
        task_id="sumo_metrics_derived__ga4_engagement_sessions_daily__v1",
        destination_table="ga4_engagement_sessions_daily_v1",
        dataset_id="sumo_metrics_derived",
        project_id="moz-fx-data-shared-prod",
        owner="plee@mozilla.com",
        email=["phlee@mozilla.com", "plee@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    sumo_metrics_derived__kitsune_questions_base__v1 = bigquery_etl_query(
        task_id="sumo_metrics_derived__kitsune_questions_base__v1",
        destination_table="kitsune_questions_base_v1",
        dataset_id="sumo_metrics_derived",
        project_id="moz-fx-data-shared-prod",
        owner="plee@mozilla.com",
        email=["phlee@mozilla.com", "plee@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    sumo_metrics_derived__translation_daily_eng_base__v1 = bigquery_etl_query(
        task_id="sumo_metrics_derived__translation_daily_eng_base__v1",
        destination_table="translation_daily_eng_base_v1",
        dataset_id="sumo_metrics_derived",
        project_id="moz-fx-data-shared-prod",
        owner="plee@mozilla.com",
        email=["phlee@mozilla.com", "plee@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    sumo_metrics_derived__translation_daily_locale_base__v1 = bigquery_etl_query(
        task_id="sumo_metrics_derived__translation_daily_locale_base__v1",
        destination_table="translation_daily_locale_base_v1",
        dataset_id="sumo_metrics_derived",
        project_id="moz-fx-data-shared-prod",
        owner="plee@mozilla.com",
        email=["phlee@mozilla.com", "plee@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    sumo_metrics_derived__zendesk_tickets_base__v1 = bigquery_etl_query(
        task_id="sumo_metrics_derived__zendesk_tickets_base__v1",
        destination_table="zendesk_tickets_base_v1",
        dataset_id="sumo_metrics_derived",
        project_id="moz-fx-data-shared-prod",
        owner="plee@mozilla.com",
        email=["phlee@mozilla.com", "plee@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    sumo_metrics_derived__ga4_engagement_sessions_daily__v1.set_upstream(
        wait_for_sumo_ga_derived__ga4_events__v1
    )
