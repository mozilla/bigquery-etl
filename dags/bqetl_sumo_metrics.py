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

    sumo_metrics_derived__kitsune_questions_base__v1 = bigquery_etl_query(
        task_id="sumo_metrics_derived__kitsune_questions_base__v1",
        destination_table="kitsune_questions_base_v1",
        dataset_id="sumo_metrics_derived",
        project_id="moz-fx-data-shared-prod",
        owner="plee@mozilla.com",
        email=["phlee@mozilla.com", "plee@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
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
