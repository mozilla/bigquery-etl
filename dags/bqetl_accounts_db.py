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
### bqetl_accounts_db

Built from bigquery-etl repo, [`dags/bqetl_accounts_db.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_accounts_db.py)

#### Description

Frequent imports from Firefox Accounts (FxA) CloudSQL databases.

Daily imports run in the `bqetl_accounts_db_daily` DAG.

#### Owner

wclouser@mozilla.com

#### Tags

* impact/tier_3
* repo/bigquery-etl
"""


default_args = {
    "owner": "wclouser@mozilla.com",
    "start_date": datetime.datetime(2026, 6, 4, 0, 0),
    "end_date": None,
    "email": [
        "wclouser@mozilla.com",
        "akomar@mozilla.com",
        "telemetry-alerts@mozilla.com",
    ],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=600),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
    "max_active_tis_per_dag": None,
}

tags = ["impact/tier_3", "repo/bigquery-etl"]

with DAG(
    "bqetl_accounts_db",
    default_args=default_args,
    schedule_interval="30 1/3 * * *",
    doc_md=docs,
    tags=tags,
    catchup=False,
) as dag:

    accounts_db_external__fxa_accounts__v1 = bigquery_etl_query(
        task_id="accounts_db_external__fxa_accounts__v1",
        destination_table="fxa_accounts_v1",
        dataset_id="accounts_db_external",
        project_id="moz-fx-data-shared-prod",
        owner="akomar@mozilla.com",
        email=[
            "akomar@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wclouser@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    with TaskGroup(
        "accounts_db_external__fxa_accounts__v1_external",
    ) as accounts_db_external__fxa_accounts__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_accounts_derived__wait_for_accounts_db_external__fxa_accounts__v1",
            external_dag_id="bqetl_accounts_derived",
            external_task_id="wait_for_accounts_db_external__fxa_accounts__v1",
            execution_date="{{ (execution_date - macros.timedelta(seconds=72000)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_accounts_db_daily__wait_for_accounts_db_external__fxa_accounts__v1",
            external_dag_id="bqetl_accounts_db_daily",
            external_task_id="wait_for_accounts_db_external__fxa_accounts__v1",
            execution_date="{{ (execution_date - macros.timedelta(seconds=75600)).isoformat() }}",
        )

        accounts_db_external__fxa_accounts__v1_external.set_upstream(
            accounts_db_external__fxa_accounts__v1
        )

    accounts_db_external__fxa_emails__v1 = bigquery_etl_query(
        task_id="accounts_db_external__fxa_emails__v1",
        destination_table="fxa_emails_v1",
        dataset_id="accounts_db_external",
        project_id="moz-fx-data-shared-prod",
        owner="akomar@mozilla.com",
        email=[
            "akomar@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wclouser@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    with TaskGroup(
        "accounts_db_external__fxa_emails__v1_external",
    ) as accounts_db_external__fxa_emails__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_accounts_derived__wait_for_accounts_db_external__fxa_emails__v1",
            external_dag_id="bqetl_accounts_derived",
            external_task_id="wait_for_accounts_db_external__fxa_emails__v1",
            execution_date="{{ (execution_date - macros.timedelta(seconds=72000)).isoformat() }}",
        )

        ExternalTaskMarker(
            task_id="bqetl_braze__wait_for_accounts_db_external__fxa_emails__v1",
            external_dag_id="bqetl_braze",
            external_task_id="wait_for_accounts_db_external__fxa_emails__v1",
        )

        accounts_db_external__fxa_emails__v1_external.set_upstream(
            accounts_db_external__fxa_emails__v1
        )

    accounts_db_external__fxa_oauth_account_authorizations__v1 = bigquery_etl_query(
        task_id="accounts_db_external__fxa_oauth_account_authorizations__v1",
        destination_table="fxa_oauth_account_authorizations_v1",
        dataset_id="accounts_db_external",
        project_id="moz-fx-data-shared-prod",
        owner="wclouser@mozilla.com",
        email=[
            "akomar@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wclouser@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    with TaskGroup(
        "accounts_db_external__fxa_oauth_account_authorizations__v1_external",
    ) as accounts_db_external__fxa_oauth_account_authorizations__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_braze__wait_for_accounts_db_external__fxa_oauth_account_authorizations__v1",
            external_dag_id="bqetl_braze",
            external_task_id="wait_for_accounts_db_external__fxa_oauth_account_authorizations__v1",
        )

        accounts_db_external__fxa_oauth_account_authorizations__v1_external.set_upstream(
            accounts_db_external__fxa_oauth_account_authorizations__v1
        )

    accounts_db_external__fxa_security_events__v1 = bigquery_etl_query(
        task_id="accounts_db_external__fxa_security_events__v1",
        destination_table="fxa_security_events_v1",
        dataset_id="accounts_db_external",
        project_id="moz-fx-data-shared-prod",
        owner="akomar@mozilla.com",
        email=[
            "akomar@mozilla.com",
            "telemetry-alerts@mozilla.com",
            "wclouser@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    with TaskGroup(
        "accounts_db_external__fxa_security_events__v1_external",
    ) as accounts_db_external__fxa_security_events__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_accounts_derived__wait_for_accounts_db_external__fxa_security_events__v1",
            external_dag_id="bqetl_accounts_derived",
            external_task_id="wait_for_accounts_db_external__fxa_security_events__v1",
            execution_date="{{ (execution_date - macros.timedelta(seconds=72000)).isoformat() }}",
        )

        accounts_db_external__fxa_security_events__v1_external.set_upstream(
            accounts_db_external__fxa_security_events__v1
        )
