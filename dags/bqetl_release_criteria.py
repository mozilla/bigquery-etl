# Generated via https://github.com/mozilla/bigquery-etl/blob/main/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskMarker
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
import datetime
from utils.constants import ALLOWED_STATES, FAILED_STATES
from utils.gcp import bigquery_etl_query, gke_command, bigquery_dq_check

docs = """
### bqetl_release_criteria

Built from bigquery-etl repo, [`dags/bqetl_release_criteria.py`](https://github.com/mozilla/bigquery-etl/blob/main/dags/bqetl_release_criteria.py)

#### Owner

perf-pmo@mozilla.com
"""


default_args = {
    "owner": "perf-pmo@mozilla.com",
    "start_date": datetime.datetime(2020, 12, 3, 0, 0),
    "end_date": None,
    "email": ["telemetry-alerts@mozilla.com", "dthorn@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
}

tags = ["impact/tier_1", "repo/bigquery-etl"]

with DAG(
    "bqetl_release_criteria",
    default_args=default_args,
    schedule_interval="@daily",
    doc_md=docs,
    tags=tags,
) as dag:
    release_criteria__dashboard_health__v1 = bigquery_etl_query(
        task_id="release_criteria__dashboard_health__v1",
        destination_table="dashboard_health_v1",
        dataset_id="release_criteria",
        project_id="moz-fx-data-bq-performance",
        owner="esmyth@mozilla.com",
        email=[
            "dthorn@mozilla.com",
            "esmyth@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        start_date=datetime.datetime(2020, 12, 3, 0, 0),
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    release_criteria__rc_flattened_test_data__v1 = bigquery_etl_query(
        task_id="release_criteria__rc_flattened_test_data__v1",
        destination_table="rc_flattened_test_data_v1",
        dataset_id="release_criteria",
        project_id="moz-fx-data-bq-performance",
        owner="esmyth@mozilla.com",
        email=[
            "dthorn@mozilla.com",
            "esmyth@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        start_date=datetime.datetime(2020, 12, 3, 0, 0),
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    release_criteria__release_criteria__v1 = bigquery_etl_query(
        task_id="release_criteria__release_criteria__v1",
        destination_table="release_criteria_v1",
        dataset_id="release_criteria",
        project_id="moz-fx-data-bq-performance",
        owner="esmyth@mozilla.com",
        email=[
            "dthorn@mozilla.com",
            "esmyth@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        start_date=datetime.datetime(2020, 12, 3, 0, 0),
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    release_criteria__release_criteria_summary__v1 = bigquery_etl_query(
        task_id="release_criteria__release_criteria_summary__v1",
        destination_table="release_criteria_summary_v1",
        dataset_id="release_criteria",
        project_id="moz-fx-data-bq-performance",
        owner="esmyth@mozilla.com",
        email=[
            "dthorn@mozilla.com",
            "esmyth@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        start_date=datetime.datetime(2020, 12, 3, 0, 0),
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    release_criteria__stale_tests__v1 = bigquery_etl_query(
        task_id="release_criteria__stale_tests__v1",
        destination_table="stale_tests_v1",
        dataset_id="release_criteria",
        project_id="moz-fx-data-bq-performance",
        owner="esmyth@mozilla.com",
        email=[
            "dthorn@mozilla.com",
            "esmyth@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        start_date=datetime.datetime(2020, 12, 3, 0, 0),
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    release_criteria__dashboard_health__v1.set_upstream(
        release_criteria__rc_flattened_test_data__v1
    )

    release_criteria__release_criteria__v1.set_upstream(
        release_criteria__rc_flattened_test_data__v1
    )

    release_criteria__release_criteria_summary__v1.set_upstream(
        release_criteria__release_criteria__v1
    )

    release_criteria__stale_tests__v1.set_upstream(
        release_criteria__release_criteria__v1
    )
