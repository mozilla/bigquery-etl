# Generated via https://github.com/mozilla/bigquery-etl/blob/main/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskMarker
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
from airflow.providers.cncf.kubernetes.secret import Secret
import datetime
from operators.gcp_container_operator import GKEPodOperator
from utils.constants import ALLOWED_STATES, FAILED_STATES
from utils.gcp import bigquery_etl_query, bigquery_dq_check, bigquery_bigeye_check

docs = """
### bqetl_google_play_store

Built from bigquery-etl repo, [`dags/bqetl_google_play_store.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_google_play_store.py)

#### Description

Schedules daily level google play store export data
#### Owner

kwindau@mozilla.com

#### Tags

* impact/tier_2
* repo/bigquery-etl
"""

google_play_store_derived__slow_startup_events_by_startup_type__v1_bqetl_google_play_store_developer_reporting_api_data_boxwood = Secret(
    deploy_type="env",
    deploy_target="GOOGLE_PLAY_STORE_SRVC_ACCT_INFO",
    secret="airflow-gke-secrets",
    key="bqetl_google_play_store_developer_reporting_api_data_boxwood",
)
google_play_store_derived__slow_startup_events_by_startup_type_and_version__v1_bqetl_google_play_store_developer_reporting_api_data_boxwood = Secret(
    deploy_type="env",
    deploy_target="GOOGLE_PLAY_STORE_SRVC_ACCT_INFO",
    secret="airflow-gke-secrets",
    key="bqetl_google_play_store_developer_reporting_api_data_boxwood",
)
google_play_store_derived__slow_startup_events_by_startup_type_version_and_device__v1_bqetl_google_play_store_developer_reporting_api_data_boxwood = Secret(
    deploy_type="env",
    deploy_target="GOOGLE_PLAY_STORE_SRVC_ACCT_INFO",
    secret="airflow-gke-secrets",
    key="bqetl_google_play_store_developer_reporting_api_data_boxwood",
)


default_args = {
    "owner": "kwindau@mozilla.com",
    "start_date": datetime.datetime(2025, 3, 18, 0, 0),
    "end_date": None,
    "email": ["kwindau@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=21600),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 4,
    "max_active_tis_per_dag": None,
}

tags = ["impact/tier_2", "repo/bigquery-etl"]

with DAG(
    "bqetl_google_play_store",
    default_args=default_args,
    schedule_interval="10 18 * * *",
    doc_md=docs,
    tags=tags,
    catchup=False,
) as dag:

    google_play_store_derived__slow_startup_events_by_startup_type__v1 = GKEPodOperator(
        task_id="google_play_store_derived__slow_startup_events_by_startup_type__v1",
        arguments=[
            "python",
            "sql/moz-fx-data-shared-prod/google_play_store_derived/slow_startup_events_by_startup_type_v1/query.py",
        ]
        + ["--date", "{{ds}}"],
        image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com"],
        secrets=[
            google_play_store_derived__slow_startup_events_by_startup_type__v1_bqetl_google_play_store_developer_reporting_api_data_boxwood,
        ],
    )

    google_play_store_derived__slow_startup_events_by_startup_type_and_version__v1 = GKEPodOperator(
        task_id="google_play_store_derived__slow_startup_events_by_startup_type_and_version__v1",
        arguments=[
            "python",
            "sql/moz-fx-data-shared-prod/google_play_store_derived/slow_startup_events_by_startup_type_and_version_v1/query.py",
        ]
        + ["--date", "{{ds}}"],
        image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com"],
        secrets=[
            google_play_store_derived__slow_startup_events_by_startup_type_and_version__v1_bqetl_google_play_store_developer_reporting_api_data_boxwood,
        ],
    )

    google_play_store_derived__slow_startup_events_by_startup_type_version_and_device__v1 = GKEPodOperator(
        task_id="google_play_store_derived__slow_startup_events_by_startup_type_version_and_device__v1",
        arguments=[
            "python",
            "sql/moz-fx-data-shared-prod/google_play_store_derived/slow_startup_events_by_startup_type_version_and_device_v1/query.py",
        ]
        + ["--date", "{{ds}}"],
        image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com"],
        secrets=[
            google_play_store_derived__slow_startup_events_by_startup_type_version_and_device__v1_bqetl_google_play_store_developer_reporting_api_data_boxwood,
        ],
    )
