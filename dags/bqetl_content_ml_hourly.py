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
### bqetl_content_ml_hourly

Built from bigquery-etl repo, [`dags/bqetl_content_ml_hourly.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_content_ml_hourly.py)

#### Description

Hourly extracts for corpus item data to be evaluated for new tab presentation.

#### Owner

jpetto@mozilla.com

#### Tags

* impact/tier_2
* repo/bigquery-etl
"""


default_args = {
    "owner": "jpetto@mozilla.com",
    "start_date": datetime.datetime(2025, 6, 24, 0, 0),
    "end_date": None,
    "email": ["jpetto@mozilla.com", "rrando@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=600),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
    "max_active_tis_per_dag": None,
}

tags = ["impact/tier_2", "repo/bigquery-etl"]

with DAG(
    "bqetl_content_ml_hourly",
    default_args=default_args,
    schedule_interval="30 * * * *",
    doc_md=docs,
    tags=tags,
    catchup=False,
) as dag:

    snowflake_migration_derived__corpus_item_schedules_updated__v1 = bigquery_etl_query(
        task_id="snowflake_migration_derived__corpus_item_schedules_updated__v1",
        destination_table="corpus_item_schedules_updated_v1",
        dataset_id="snowflake_migration_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jpetto@mozilla.com",
        email=["jpetto@mozilla.com", "rrando@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    snowflake_migration_derived__corpus_items_updated__v1 = bigquery_etl_query(
        task_id="snowflake_migration_derived__corpus_items_updated__v1",
        destination_table="corpus_items_updated_v1",
        dataset_id="snowflake_migration_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jpetto@mozilla.com",
        email=["jpetto@mozilla.com", "rrando@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    snowflake_migration_derived__dismissed_prospects__v1 = bigquery_etl_query(
        task_id="snowflake_migration_derived__dismissed_prospects__v1",
        destination_table="dismissed_prospects_v1",
        dataset_id="snowflake_migration_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jpetto@mozilla.com",
        email=["jpetto@mozilla.com", "rrando@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    snowflake_migration_derived__prospect_item_feed__v1 = bigquery_etl_query(
        task_id="snowflake_migration_derived__prospect_item_feed__v1",
        destination_table="prospect_item_feed_v1",
        dataset_id="snowflake_migration_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jpetto@mozilla.com",
        email=["jpetto@mozilla.com", "rrando@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    snowflake_migration_derived__prospects__v1 = bigquery_etl_query(
        task_id="snowflake_migration_derived__prospects__v1",
        destination_table="prospects_v1",
        dataset_id="snowflake_migration_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jpetto@mozilla.com",
        email=["jpetto@mozilla.com", "rrando@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    snowflake_migration_derived__rejected_corpus_items__v1 = bigquery_etl_query(
        task_id="snowflake_migration_derived__rejected_corpus_items__v1",
        destination_table="rejected_corpus_items_v1",
        dataset_id="snowflake_migration_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jpetto@mozilla.com",
        email=["jpetto@mozilla.com", "rrando@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    snowflake_migration_derived__scheduled_articles_report__v1 = bigquery_etl_query(
        task_id="snowflake_migration_derived__scheduled_articles_report__v1",
        destination_table="scheduled_articles_report_v1",
        dataset_id="snowflake_migration_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jpetto@mozilla.com",
        email=["jpetto@mozilla.com", "rrando@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    snowflake_migration_derived__stg_reviewed_corpus_items__v1 = bigquery_etl_query(
        task_id="snowflake_migration_derived__stg_reviewed_corpus_items__v1",
        destination_table="stg_reviewed_corpus_items_v1",
        dataset_id="snowflake_migration_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jpetto@mozilla.com",
        email=["jpetto@mozilla.com", "rrando@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    snowflake_migration_derived__stg_scheduled_corpus_items__v1 = bigquery_etl_query(
        task_id="snowflake_migration_derived__stg_scheduled_corpus_items__v1",
        destination_table="stg_scheduled_corpus_items_v1",
        dataset_id="snowflake_migration_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jpetto@mozilla.com",
        email=["jpetto@mozilla.com", "rrando@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    snowflake_migration_derived__corpus_item_schedules_updated__v1.set_upstream(
        snowflake_migration_derived__corpus_items_updated__v1
    )

    snowflake_migration_derived__corpus_item_schedules_updated__v1.set_upstream(
        snowflake_migration_derived__stg_scheduled_corpus_items__v1
    )

    snowflake_migration_derived__corpus_items_updated__v1.set_upstream(
        snowflake_migration_derived__prospect_item_feed__v1
    )

    snowflake_migration_derived__corpus_items_updated__v1.set_upstream(
        snowflake_migration_derived__stg_reviewed_corpus_items__v1
    )

    snowflake_migration_derived__dismissed_prospects__v1.set_upstream(
        snowflake_migration_derived__corpus_items_updated__v1
    )

    snowflake_migration_derived__dismissed_prospects__v1.set_upstream(
        snowflake_migration_derived__prospects__v1
    )

    snowflake_migration_derived__dismissed_prospects__v1.set_upstream(
        snowflake_migration_derived__rejected_corpus_items__v1
    )

    snowflake_migration_derived__prospect_item_feed__v1.set_upstream(
        snowflake_migration_derived__prospects__v1
    )

    snowflake_migration_derived__rejected_corpus_items__v1.set_upstream(
        snowflake_migration_derived__prospect_item_feed__v1
    )

    snowflake_migration_derived__rejected_corpus_items__v1.set_upstream(
        snowflake_migration_derived__stg_reviewed_corpus_items__v1
    )

    snowflake_migration_derived__scheduled_articles_report__v1.set_upstream(
        snowflake_migration_derived__corpus_item_schedules_updated__v1
    )

    snowflake_migration_derived__scheduled_articles_report__v1.set_upstream(
        snowflake_migration_derived__corpus_items_updated__v1
    )
