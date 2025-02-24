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
### bqetl_merino_newtab_extract_to_gcs

Built from bigquery-etl repo, [`dags/bqetl_merino_newtab_extract_to_gcs.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_merino_newtab_extract_to_gcs.py)

#### Description

Aggregates Newtab engagement data that lands in a GCS bucket for Merino recommendations.

#### Owner

cbeck@mozilla.com

#### Tags

* impact/tier_1
* repo/bigquery-etl
"""


default_args = {
    "owner": "cbeck@mozilla.com",
    "start_date": datetime.datetime(2024, 8, 14, 0, 0),
    "end_date": None,
    "email": ["cbeck@mozilla.com", "gkatre@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=300),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
}

tags = ["impact/tier_1", "repo/bigquery-etl"]

with DAG(
    "bqetl_merino_newtab_extract_to_gcs",
    default_args=default_args,
    schedule_interval="*/20 * * * *",
    doc_md=docs,
    tags=tags,
    catchup=False,
) as dag:

    checks__fail_telemetry_derived__newtab_merino_extract__v1 = bigquery_dq_check(
        task_id="checks__fail_telemetry_derived__newtab_merino_extract__v1",
        source_table="newtab_merino_extract_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        is_dq_check_fail=True,
        owner="cbeck@mozilla.com",
        email=["cbeck@mozilla.com", "gkatre@mozilla.com"],
        depends_on_past=False,
        task_concurrency=1,
        retries=0,
    )

    telemetry_derived__newtab_merino_extract__v1 = bigquery_etl_query(
        task_id="telemetry_derived__newtab_merino_extract__v1",
        destination_table="newtab_merino_extract_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="cbeck@mozilla.com",
        email=["cbeck@mozilla.com", "gkatre@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    telemetry_derived__newtab_merino_extract_to_gcs__v1 = GKEPodOperator(
        task_id="telemetry_derived__newtab_merino_extract_to_gcs__v1",
        arguments=[
            "python",
            "sql/moz-fx-data-shared-prod/telemetry_derived/newtab_merino_extract_to_gcs_v1/query.py",
        ]
        + [
            "--source-project=moz-fx-data-shared-prod",
            "--source-dataset=telemetry_derived",
            "--source-table=newtab_merino_extract_v1",
            "--destination-bucket=merino-airflow-data-prodpy",
            "--destination-prefix=newtab-merino-exports/engagement",
            "--deletion-days-old=3",
        ],
        image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
        owner="cbeck@mozilla.com",
        email=["cbeck@mozilla.com", "gkatre@mozilla.com"],
    )

    checks__fail_telemetry_derived__newtab_merino_extract__v1.set_upstream(
        telemetry_derived__newtab_merino_extract__v1
    )

    telemetry_derived__newtab_merino_extract_to_gcs__v1.set_upstream(
        checks__fail_telemetry_derived__newtab_merino_extract__v1
    )
