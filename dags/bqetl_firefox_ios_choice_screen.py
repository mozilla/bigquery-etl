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
### bqetl_firefox_ios_choice_screen

Built from bigquery-etl repo, [`dags/bqetl_firefox_ios_choice_screen.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_firefox_ios_choice_screen.py)

#### Description

Job to pull user choice screen info from Apple.
#### Owner

kik@mozilla.com

#### Tags

* impact/tier_3
* repo/bigquery-etl
* triage/no_triage
"""

firefox_ios_derived__app_store_choice_screen_engagement__v1_bqetl_firefox_ios__app_store_connect_issuer_id = Secret(
    deploy_type="env",
    deploy_target="CONNECT_ISSUER_ID",
    secret="airflow-gke-secrets",
    key="bqetl_firefox_ios__app_store_connect_issuer_id",
)
firefox_ios_derived__app_store_choice_screen_engagement__v1_bqetl_firefox_ios__app_store_connect_key_id = Secret(
    deploy_type="env",
    deploy_target="CONNECT_KEY_ID",
    secret="airflow-gke-secrets",
    key="bqetl_firefox_ios__app_store_connect_key_id",
)
firefox_ios_derived__app_store_choice_screen_engagement__v1_bqetl_firefox_ios__app_store_connect_key = Secret(
    deploy_type="env",
    deploy_target="CONNECT_KEY",
    secret="airflow-gke-secrets",
    key="bqetl_firefox_ios__app_store_connect_key",
)
firefox_ios_derived__app_store_choice_screen_selection__v1_bqetl_firefox_ios__app_store_connect_issuer_id = Secret(
    deploy_type="env",
    deploy_target="CONNECT_ISSUER_ID",
    secret="airflow-gke-secrets",
    key="bqetl_firefox_ios__app_store_connect_issuer_id",
)
firefox_ios_derived__app_store_choice_screen_selection__v1_bqetl_firefox_ios__app_store_connect_key_id = Secret(
    deploy_type="env",
    deploy_target="CONNECT_KEY_ID",
    secret="airflow-gke-secrets",
    key="bqetl_firefox_ios__app_store_connect_key_id",
)
firefox_ios_derived__app_store_choice_screen_selection__v1_bqetl_firefox_ios__app_store_connect_key = Secret(
    deploy_type="env",
    deploy_target="CONNECT_KEY",
    secret="airflow-gke-secrets",
    key="bqetl_firefox_ios__app_store_connect_key",
)


default_args = {
    "owner": "kik@mozilla.com",
    "start_date": datetime.datetime(2025, 1, 14, 0, 0),
    "end_date": None,
    "email": ["kik@mozilla.com", "ebrandi@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=600),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "max_active_tis_per_dag": None,
}

tags = ["impact/tier_3", "repo/bigquery-etl", "triage/no_triage"]

with DAG(
    "bqetl_firefox_ios_choice_screen",
    default_args=default_args,
    schedule_interval="@daily",
    doc_md=docs,
    tags=tags,
    catchup=True,
) as dag:

    firefox_ios_derived__app_store_choice_screen_engagement__v1 = GKEPodOperator(
        task_id="firefox_ios_derived__app_store_choice_screen_engagement__v1",
        arguments=[
            "python",
            "sql/moz-fx-data-shared-prod/firefox_ios_derived/app_store_choice_screen_engagement_v1/query.py",
        ]
        + [
            "--date={{ds}}",
            "--connect_app_id=989804926",
            "--partition_field=logical_date",
        ],
        image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
        owner="kik@mozilla.com",
        email=["ebrandi@mozilla.com", "kik@mozilla.com"],
        secrets=[
            firefox_ios_derived__app_store_choice_screen_engagement__v1_bqetl_firefox_ios__app_store_connect_issuer_id,
            firefox_ios_derived__app_store_choice_screen_engagement__v1_bqetl_firefox_ios__app_store_connect_key_id,
            firefox_ios_derived__app_store_choice_screen_engagement__v1_bqetl_firefox_ios__app_store_connect_key,
        ],
    )

    firefox_ios_derived__app_store_choice_screen_selection__v1 = GKEPodOperator(
        task_id="firefox_ios_derived__app_store_choice_screen_selection__v1",
        arguments=[
            "python",
            "sql/moz-fx-data-shared-prod/firefox_ios_derived/app_store_choice_screen_selection_v1/query.py",
        ]
        + [
            "--date={{macros.ds_add(ds, -10)}}",
            "--connect_app_id=989804926",
            "--partition_field=logical_date",
        ],
        image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
        owner="kik@mozilla.com",
        email=["ebrandi@mozilla.com", "kik@mozilla.com"],
        secrets=[
            firefox_ios_derived__app_store_choice_screen_selection__v1_bqetl_firefox_ios__app_store_connect_issuer_id,
            firefox_ios_derived__app_store_choice_screen_selection__v1_bqetl_firefox_ios__app_store_connect_key_id,
            firefox_ios_derived__app_store_choice_screen_selection__v1_bqetl_firefox_ios__app_store_connect_key,
        ],
        retry_delay=datetime.timedelta(seconds=1800),
        retries=2,
        email_on_retry=False,
    )
