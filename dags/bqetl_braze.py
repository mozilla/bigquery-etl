# Generated via https://github.com/mozilla/bigquery-etl/blob/main/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskMarker
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
import datetime
from operators.gcp_container_operator import GKEPodOperator
from utils.constants import ALLOWED_STATES, FAILED_STATES
from utils.gcp import bigquery_etl_query, bigquery_dq_check

docs = """
### bqetl_braze

Built from bigquery-etl repo, [`dags/bqetl_braze.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_braze.py)

#### Description

ETL for Braze workflows.

#### Owner

cbeck@mozilla.com

#### Tags

* impact/tier_2
* repo/bigquery-etl
"""


default_args = {
    "owner": "cbeck@mozilla.com",
    "start_date": datetime.datetime(2024, 4, 15, 0, 0),
    "end_date": None,
    "email": ["cbeck@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
}

tags = ["impact/tier_2", "repo/bigquery-etl"]

with DAG(
    "bqetl_braze",
    default_args=default_args,
    schedule_interval="0 10 * * *",
    doc_md=docs,
    tags=tags,
) as dag:

    wait_for_subscription_platform_derived__stripe_subscriptions__v1 = (
        ExternalTaskSensor(
            task_id="wait_for_subscription_platform_derived__stripe_subscriptions__v1",
            external_dag_id="bqetl_subplat",
            external_task_id="subscription_platform_derived__stripe_subscriptions__v1",
            execution_delta=datetime.timedelta(seconds=29700),
            check_existence=True,
            mode="reschedule",
            allowed_states=ALLOWED_STATES,
            failed_states=FAILED_STATES,
            pool="DATA_ENG_EXTERNALTASKSENSOR",
        )
    )

    wait_for_acoustic_external__contact_raw__v1 = ExternalTaskSensor(
        task_id="wait_for_acoustic_external__contact_raw__v1",
        external_dag_id="bqetl_acoustic_contact_export",
        external_task_id="acoustic_external__contact_raw__v1",
        execution_delta=datetime.timedelta(seconds=3600),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    wait_for_acoustic_external__suppression_list__v1 = ExternalTaskSensor(
        task_id="wait_for_acoustic_external__suppression_list__v1",
        external_dag_id="bqetl_acoustic_suppression_list",
        external_task_id="acoustic_external__suppression_list__v1",
        execution_delta=datetime.timedelta(seconds=3600),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    braze_derived__newsletters__v1 = bigquery_etl_query(
        task_id="braze_derived__newsletters__v1",
        destination_table="newsletters_v1",
        dataset_id="braze_derived",
        project_id="moz-fx-data-shared-prod",
        owner="cbeck@mozilla.com",
        email=["cbeck@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    braze_derived__products__v1 = bigquery_etl_query(
        task_id="braze_derived__products__v1",
        destination_table="products_v1",
        dataset_id="braze_derived",
        project_id="moz-fx-data-shared-prod",
        owner="cbeck@mozilla.com",
        email=["cbeck@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    braze_derived__subscriptions__v1 = bigquery_etl_query(
        task_id="braze_derived__subscriptions__v1",
        destination_table="subscriptions_v1",
        dataset_id="braze_derived",
        project_id="moz-fx-data-shared-prod",
        owner="cbeck@mozilla.com",
        email=["cbeck@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    braze_derived__suppressions__v1 = bigquery_etl_query(
        task_id="braze_derived__suppressions__v1",
        destination_table="suppressions_v1",
        dataset_id="braze_derived",
        project_id="moz-fx-data-shared-prod",
        owner="cbeck@mozilla.com",
        email=["cbeck@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    braze_derived__user_profiles__v1 = bigquery_etl_query(
        task_id="braze_derived__user_profiles__v1",
        destination_table="user_profiles_v1",
        dataset_id="braze_derived",
        project_id="moz-fx-data-shared-prod",
        owner="cbeck@mozilla.com",
        email=["cbeck@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    braze_derived__users__v1 = bigquery_etl_query(
        task_id="braze_derived__users__v1",
        destination_table="users_v1",
        dataset_id="braze_derived",
        project_id="moz-fx-data-shared-prod",
        owner="cbeck@mozilla.com",
        email=["cbeck@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    braze_derived__waitlists__v1 = bigquery_etl_query(
        task_id="braze_derived__waitlists__v1",
        destination_table="waitlists_v1",
        dataset_id="braze_derived",
        project_id="moz-fx-data-shared-prod",
        owner="cbeck@mozilla.com",
        email=["cbeck@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    braze_external__changed_newsletters_sync__v1 = bigquery_etl_query(
        task_id="braze_external__changed_newsletters_sync__v1",
        destination_table="changed_newsletters_sync_v1",
        dataset_id="braze_external",
        project_id="moz-fx-data-shared-prod",
        owner="cbeck@mozilla.com",
        email=["cbeck@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    braze_external__changed_subscriptions_sync__v1 = bigquery_etl_query(
        task_id="braze_external__changed_subscriptions_sync__v1",
        destination_table="changed_subscriptions_sync_v1",
        dataset_id="braze_external",
        project_id="moz-fx-data-shared-prod",
        owner="cbeck@mozilla.com",
        email=["cbeck@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    braze_external__changed_users__v1 = bigquery_etl_query(
        task_id="braze_external__changed_users__v1",
        destination_table="changed_users_v1",
        dataset_id="braze_external",
        project_id="moz-fx-data-shared-prod",
        owner="cbeck@mozilla.com",
        email=["cbeck@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    braze_external__changed_users_sync__v1 = bigquery_etl_query(
        task_id="braze_external__changed_users_sync__v1",
        destination_table="changed_users_sync_v1",
        dataset_id="braze_external",
        project_id="moz-fx-data-shared-prod",
        owner="cbeck@mozilla.com",
        email=["cbeck@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    braze_external__changed_waitlists_sync__v1 = bigquery_etl_query(
        task_id="braze_external__changed_waitlists_sync__v1",
        destination_table="changed_waitlists_sync_v1",
        dataset_id="braze_external",
        project_id="moz-fx-data-shared-prod",
        owner="cbeck@mozilla.com",
        email=["cbeck@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    braze_external__delete_users_sync__v1 = bigquery_etl_query(
        task_id="braze_external__delete_users_sync__v1",
        destination_table="delete_users_sync_v1",
        dataset_id="braze_external",
        project_id="moz-fx-data-shared-prod",
        owner="cbeck@mozilla.com",
        email=["cbeck@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    braze_external__users_previous_day_snapshot__v1 = bigquery_etl_query(
        task_id="braze_external__users_previous_day_snapshot__v1",
        destination_table=None,
        dataset_id="braze_external",
        project_id="moz-fx-data-shared-prod",
        owner="cbeck@mozilla.com",
        email=["cbeck@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        sql_file_path="sql/moz-fx-data-shared-prod/braze_external/users_previous_day_snapshot_v1/script.sql",
    )

    braze_derived__newsletters__v1.set_upstream(braze_derived__users__v1)

    braze_derived__products__v1.set_upstream(braze_derived__users__v1)

    braze_derived__products__v1.set_upstream(
        wait_for_subscription_platform_derived__stripe_subscriptions__v1
    )

    braze_derived__subscriptions__v1.set_upstream(braze_derived__user_profiles__v1)

    braze_derived__subscriptions__v1.set_upstream(braze_derived__users__v1)

    braze_derived__suppressions__v1.set_upstream(
        wait_for_acoustic_external__contact_raw__v1
    )

    braze_derived__suppressions__v1.set_upstream(
        wait_for_acoustic_external__suppression_list__v1
    )

    braze_derived__user_profiles__v1.set_upstream(braze_derived__newsletters__v1)

    braze_derived__user_profiles__v1.set_upstream(braze_derived__products__v1)

    braze_derived__user_profiles__v1.set_upstream(braze_derived__users__v1)

    braze_derived__user_profiles__v1.set_upstream(braze_derived__waitlists__v1)

    braze_derived__users__v1.set_upstream(braze_derived__suppressions__v1)

    braze_derived__users__v1.set_upstream(
        wait_for_subscription_platform_derived__stripe_subscriptions__v1
    )

    braze_external__changed_newsletters_sync__v1.set_upstream(
        braze_derived__newsletters__v1
    )

    braze_external__changed_subscriptions_sync__v1.set_upstream(
        braze_derived__subscriptions__v1
    )

    braze_external__changed_users__v1.set_upstream(braze_derived__users__v1)

    braze_external__changed_users__v1.set_upstream(
        braze_external__users_previous_day_snapshot__v1
    )

    braze_external__changed_users_sync__v1.set_upstream(
        braze_external__changed_users__v1
    )

    braze_external__changed_waitlists_sync__v1.set_upstream(
        braze_derived__waitlists__v1
    )

    braze_external__delete_users_sync__v1.set_upstream(
        braze_external__changed_users__v1
    )
