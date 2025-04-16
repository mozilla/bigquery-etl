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
### bqetl_imf_cpi_data

Built from bigquery-etl repo, [`dags/bqetl_imf_cpi_data.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_imf_cpi_data.py)

#### Description

This DAG pulls inflation data from the International Monetary Fund CPI API

#### Owner

kwindau@mozilla.com

#### Tags

* impact/tier_3
* repo/bigquery-etl
"""


default_args = {
    "owner": "kwindau@mozilla.com",
    "start_date": datetime.datetime(2025, 1, 1, 0, 0),
    "end_date": None,
    "email": ["kwindau@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1500),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "max_active_tis_per_dag": None,
}

tags = ["impact/tier_3", "repo/bigquery-etl"]

with DAG(
    "bqetl_imf_cpi_data",
    default_args=default_args,
    schedule_interval="23 0 15 * *",
    doc_md=docs,
    tags=tags,
    catchup=False,
) as dag:

    checks__fail_external_derived__monthly_inflation__v1 = bigquery_dq_check(
        task_id="checks__fail_external_derived__monthly_inflation__v1",
        source_table="monthly_inflation_v1",
        dataset_id="external_derived",
        project_id="moz-fx-data-shared-prod",
        is_dq_check_fail=True,
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com"],
        depends_on_past=False,
        parameters=["submission_date:DATE:{{ds}}"],
        retries=0,
    )

    checks__fail_external_derived__quarterly_inflation__v1 = bigquery_dq_check(
        task_id="checks__fail_external_derived__quarterly_inflation__v1",
        source_table="quarterly_inflation_v1",
        dataset_id="external_derived",
        project_id="moz-fx-data-shared-prod",
        is_dq_check_fail=True,
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com"],
        depends_on_past=False,
        parameters=["submission_date:DATE:{{ds}}"],
        retries=0,
    )

    external_derived__gdp__v1 = GKEPodOperator(
        task_id="external_derived__gdp__v1",
        arguments=[
            "python",
            "sql/moz-fx-data-shared-prod/external_derived/gdp_v1/query.py",
        ]
        + [],
        image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com"],
    )

    external_derived__monthly_inflation__v1 = GKEPodOperator(
        task_id="external_derived__monthly_inflation__v1",
        arguments=[
            "python",
            "sql/moz-fx-data-shared-prod/external_derived/monthly_inflation_v1/query.py",
        ]
        + [],
        image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com"],
    )

    external_derived__quarterly_inflation__v1 = GKEPodOperator(
        task_id="external_derived__quarterly_inflation__v1",
        arguments=[
            "python",
            "sql/moz-fx-data-shared-prod/external_derived/quarterly_inflation_v1/query.py",
        ]
        + [],
        image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
        owner="kwindau@mozilla.com",
        email=["kwindau@mozilla.com"],
    )

    checks__fail_external_derived__monthly_inflation__v1.set_upstream(
        external_derived__monthly_inflation__v1
    )

    checks__fail_external_derived__quarterly_inflation__v1.set_upstream(
        external_derived__quarterly_inflation__v1
    )
