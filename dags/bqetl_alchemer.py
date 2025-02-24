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
### bqetl_alchemer

Built from bigquery-etl repo, [`dags/bqetl_alchemer.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_alchemer.py)

#### Description

DAG for scheduling queries pulling survey data from Alchemer.

#### Owner

ago@mozilla.com

#### Tags

* impact/tier_3
* repo/bigquery-etl
"""


default_args = {
    "owner": "ago@mozilla.com",
    "start_date": datetime.datetime(2023, 6, 1, 0, 0),
    "end_date": None,
    "email": ["ago@mozilla.com", "eshallal@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=3600),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
}

tags = ["impact/tier_3", "repo/bigquery-etl"]

with DAG(
    "bqetl_alchemer",
    default_args=default_args,
    schedule_interval="0 12 * * *",
    doc_md=docs,
    tags=tags,
    catchup=True,
) as dag:

    fx_quant_user_research_derived__fxqur_viewpoint_desktop__v1 = GKEPodOperator(
        task_id="fx_quant_user_research_derived__fxqur_viewpoint_desktop__v1",
        arguments=[
            "python",
            "sql/moz-fx-data-shared-prod/fx_quant_user_research_derived/fxqur_viewpoint_desktop_v1/query.py",
        ]
        + [
            "--date",
            "{{ ds }}",
            "--survey_id",
            "7317764",
            "--api_token",
            "{{ var.value.surveygizmo_api_token }}",
            "--api_secret",
            "{{ var.value.surveygizmo_api_secret }}",
            "--destination_table",
            "moz-fx-data-shared-prod.fx_quant_user_research_derived.fxqur_viewpoint_desktop_v1",
        ],
        image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
        owner="ago@mozilla.com",
        email=["ago@mozilla.com", "eshallal@mozilla.com"],
    )

    fx_quant_user_research_derived__fxqur_viewpoint_mobile__v1 = GKEPodOperator(
        task_id="fx_quant_user_research_derived__fxqur_viewpoint_mobile__v1",
        arguments=[
            "python",
            "sql/moz-fx-data-shared-prod/fx_quant_user_research_derived/fxqur_viewpoint_mobile_v1/query.py",
        ]
        + [
            "--date",
            "{{ ds }}",
            "--survey_id",
            "7355778",
            "--api_token",
            "{{ var.value.surveygizmo_api_token }}",
            "--api_secret",
            "{{ var.value.surveygizmo_api_secret }}",
            "--destination_table",
            "moz-fx-data-shared-prod.fx_quant_user_research_derived.fxqur_viewpoint_mobile_v1",
        ],
        image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
        owner="ago@mozilla.com",
        email=["ago@mozilla.com", "eshallal@mozilla.com"],
    )
