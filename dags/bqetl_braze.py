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
### bqetl_braze

Built from bigquery-etl repo, [`dags/bqetl_braze.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_braze.py)

#### Description

ETL for Braze workflows.
#### Owner

lmcfall@mozilla.com

#### Tags

* impact/tier_2
* repo/bigquery-etl
"""


default_args = {
    "owner": "lmcfall@mozilla.com",
    "start_date": datetime.datetime(2024, 4, 15, 0, 0),
    "end_date": None,
    "email": [
        "cbeck@mozilla.com",
        "lmcfall@mozilla.com",
        "sherrera@mozilla.com",
        "telemetry-alerts@mozilla.com",
    ],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=300),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 3,
    "max_active_tis_per_dag": None,
}

tags = ["impact/tier_2", "repo/bigquery-etl"]

with DAG(
    "bqetl_braze",
    default_args=default_args,
    schedule_interval="0 5,13,21 * * *",
    doc_md=docs,
    tags=tags,
    catchup=False,
) as dag:

    wait_for_accounts_db_external__fxa_emails__v1 = ExternalTaskSensor(
        task_id="wait_for_accounts_db_external__fxa_emails__v1",
        external_dag_id="bqetl_accounts_db",
        external_task_id="accounts_db_external__fxa_emails__v1",
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_accounts_db_external__fxa_oauth_account_authorizations__v1 = ExternalTaskSensor(
        task_id="wait_for_accounts_db_external__fxa_oauth_account_authorizations__v1",
        external_dag_id="bqetl_accounts_db",
        external_task_id="accounts_db_external__fxa_oauth_account_authorizations__v1",
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    braze_derived__fxa_services__v1 = bigquery_etl_query(
        task_id="braze_derived__fxa_services__v1",
        destination_table="fxa_services_v1",
        dataset_id="braze_derived",
        project_id="moz-fx-data-shared-prod",
        owner="sherrera@mozilla.com",
        email=[
            "cbeck@mozilla.com",
            "lmcfall@mozilla.com",
            "sherrera@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    braze_derived__products__v2 = bigquery_etl_query(
        task_id="braze_derived__products__v2",
        destination_table="products_v2",
        dataset_id="braze_derived",
        project_id="moz-fx-data-shared-prod",
        owner="cbeck@mozilla.com",
        email=[
            "cbeck@mozilla.com",
            "lmcfall@mozilla.com",
            "sherrera@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    braze_external__changed_fxa_services_events_sync__v1 = bigquery_etl_query(
        task_id="braze_external__changed_fxa_services_events_sync__v1",
        destination_table="changed_fxa_services_events_sync_v1",
        dataset_id="braze_external",
        project_id="moz-fx-data-shared-prod",
        owner="sherrera@mozilla.com",
        email=[
            "cbeck@mozilla.com",
            "lmcfall@mozilla.com",
            "sherrera@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
        arguments=["--append_table", "--noreplace"],
    )

    braze_external__changed_fxa_services_sync__v1 = bigquery_etl_query(
        task_id="braze_external__changed_fxa_services_sync__v1",
        destination_table="changed_fxa_services_sync_v1",
        dataset_id="braze_external",
        project_id="moz-fx-data-shared-prod",
        owner="sherrera@mozilla.com",
        email=[
            "cbeck@mozilla.com",
            "lmcfall@mozilla.com",
            "sherrera@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
        arguments=["--append_table", "--noreplace"],
    )

    braze_external__changed_products_sync__v2 = bigquery_etl_query(
        task_id="braze_external__changed_products_sync__v2",
        destination_table="changed_products_sync_v2",
        dataset_id="braze_external",
        project_id="moz-fx-data-shared-prod",
        owner="cbeck@mozilla.com",
        email=[
            "cbeck@mozilla.com",
            "lmcfall@mozilla.com",
            "sherrera@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
        arguments=["--append_table", "--noreplace"],
    )

    checks__fail_braze_derived__fxa_services__v1 = bigquery_dq_check(
        task_id="checks__fail_braze_derived__fxa_services__v1",
        source_table="fxa_services_v1",
        dataset_id="braze_derived",
        project_id="moz-fx-data-shared-prod",
        is_dq_check_fail=True,
        owner="sherrera@mozilla.com",
        email=[
            "cbeck@mozilla.com",
            "lmcfall@mozilla.com",
            "sherrera@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        depends_on_past=False,
        task_concurrency=1,
        retry_delay=datetime.timedelta(seconds=300),
        retries=1,
    )

    checks__fail_braze_derived__products__v2 = bigquery_dq_check(
        task_id="checks__fail_braze_derived__products__v2",
        source_table="products_v2",
        dataset_id="braze_derived",
        project_id="moz-fx-data-shared-prod",
        is_dq_check_fail=True,
        owner="cbeck@mozilla.com",
        email=[
            "cbeck@mozilla.com",
            "lmcfall@mozilla.com",
            "sherrera@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        depends_on_past=False,
        task_concurrency=1,
        retry_delay=datetime.timedelta(seconds=300),
        retries=1,
    )

    checks__warn_braze_derived__fxa_services__v1 = bigquery_dq_check(
        task_id="checks__warn_braze_derived__fxa_services__v1",
        source_table="fxa_services_v1",
        dataset_id="braze_derived",
        project_id="moz-fx-data-shared-prod",
        is_dq_check_fail=False,
        owner="sherrera@mozilla.com",
        email=[
            "cbeck@mozilla.com",
            "lmcfall@mozilla.com",
            "sherrera@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        depends_on_past=False,
        task_concurrency=1,
        retry_delay=datetime.timedelta(seconds=300),
        retries=1,
    )

    braze_derived__fxa_services__v1.set_upstream(
        wait_for_accounts_db_external__fxa_emails__v1
    )

    braze_derived__fxa_services__v1.set_upstream(
        wait_for_accounts_db_external__fxa_oauth_account_authorizations__v1
    )

    braze_external__changed_fxa_services_events_sync__v1.set_upstream(
        checks__fail_braze_derived__fxa_services__v1
    )

    braze_external__changed_fxa_services_sync__v1.set_upstream(
        checks__fail_braze_derived__fxa_services__v1
    )

    braze_external__changed_products_sync__v2.set_upstream(
        checks__fail_braze_derived__products__v2
    )

    checks__fail_braze_derived__fxa_services__v1.set_upstream(
        braze_derived__fxa_services__v1
    )

    checks__fail_braze_derived__products__v2.set_upstream(braze_derived__products__v2)

    checks__warn_braze_derived__fxa_services__v1.set_upstream(
        braze_derived__fxa_services__v1
    )
