# Generated via https://github.com/mozilla/bigquery-etl/blob/main/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskMarker
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
import datetime
from operators.gcp_container_operator import GKEPodOperator
from utils.constants import ALLOWED_STATES, FAILED_STATES
from utils.gcp import bigquery_etl_query, bigquery_dq_check, bigquery_bigeye_check

from fivetran_provider_async.operators import FivetranOperator

docs = """
### bqetl_subplat_hourly

Built from bigquery-etl repo, [`dags/bqetl_subplat_hourly.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_subplat_hourly.py)

#### Description

Hourly imports for Subscription Platform data from Stripe and Firestore,
as well as derived tables based on that data.

This DAG is scheduled with consideration for its ETLs that write to date-partitioned tables,
so the last run targeting a particular date partition will happen at 00:30 the following day,
allowing time for all of the data for that date to be ready from external systems like Stripe.

#### Owner

srose@mozilla.com

#### Tags

* impact/tier_1
* repo/bigquery-etl
"""


default_args = {
    "owner": "srose@mozilla.com",
    "start_date": datetime.datetime(2025, 6, 10, 0, 0),
    "end_date": None,
    "email": ["telemetry-alerts@mozilla.com", "srose@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=300),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
    "max_active_tis_per_dag": None,
}

tags = ["impact/tier_1", "repo/bigquery-etl"]

with DAG(
    "bqetl_subplat_hourly",
    default_args=default_args,
    schedule_interval="30 * * * *",
    doc_md=docs,
    tags=tags,
    catchup=True,
) as dag:

    fivetran_stripe_sync_start = FivetranOperator(
        connector_id="{{ var.value.fivetran_stripe_connector_id }}",
        task_id="fivetran_stripe_task",
        deferrable=False,
        task_concurrency=1,
    )

    bigeye__subscription_platform_derived__apple_logical_subscriptions_history__v1 = bigquery_bigeye_check(
        task_id="bigeye__subscription_platform_derived__apple_logical_subscriptions_history__v1",
        table_id="moz-fx-data-shared-prod.subscription_platform_derived.apple_logical_subscriptions_history_v1",
        warehouse_id="1939",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
    )

    bigeye__subscription_platform_derived__apple_subscriptions_changelog__v1 = bigquery_bigeye_check(
        task_id="bigeye__subscription_platform_derived__apple_subscriptions_changelog__v1",
        table_id="moz-fx-data-shared-prod.subscription_platform_derived.apple_subscriptions_changelog_v1",
        warehouse_id="1939",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
    )

    bigeye__subscription_platform_derived__apple_subscriptions_history__v1 = bigquery_bigeye_check(
        task_id="bigeye__subscription_platform_derived__apple_subscriptions_history__v1",
        table_id="moz-fx-data-shared-prod.subscription_platform_derived.apple_subscriptions_history_v1",
        warehouse_id="1939",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
    )

    bigeye__subscription_platform_derived__apple_subscriptions_revised_changelog__v1 = bigquery_bigeye_check(
        task_id="bigeye__subscription_platform_derived__apple_subscriptions_revised_changelog__v1",
        table_id="moz-fx-data-shared-prod.subscription_platform_derived.apple_subscriptions_revised_changelog_v1",
        warehouse_id="1939",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
    )

    bigeye__subscription_platform_derived__google_logical_subscriptions_history__v1 = bigquery_bigeye_check(
        task_id="bigeye__subscription_platform_derived__google_logical_subscriptions_history__v1",
        table_id="moz-fx-data-shared-prod.subscription_platform_derived.google_logical_subscriptions_history_v1",
        warehouse_id="1939",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
    )

    bigeye__subscription_platform_derived__google_subscriptions_changelog__v1 = bigquery_bigeye_check(
        task_id="bigeye__subscription_platform_derived__google_subscriptions_changelog__v1",
        table_id="moz-fx-data-shared-prod.subscription_platform_derived.google_subscriptions_changelog_v1",
        warehouse_id="1939",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
    )

    bigeye__subscription_platform_derived__google_subscriptions_history__v1 = bigquery_bigeye_check(
        task_id="bigeye__subscription_platform_derived__google_subscriptions_history__v1",
        table_id="moz-fx-data-shared-prod.subscription_platform_derived.google_subscriptions_history_v1",
        warehouse_id="1939",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
    )

    bigeye__subscription_platform_derived__google_subscriptions_revised_changelog__v1 = bigquery_bigeye_check(
        task_id="bigeye__subscription_platform_derived__google_subscriptions_revised_changelog__v1",
        table_id="moz-fx-data-shared-prod.subscription_platform_derived.google_subscriptions_revised_changelog_v1",
        warehouse_id="1939",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
    )

    bigeye__subscription_platform_derived__stripe_customers_history__v1 = bigquery_bigeye_check(
        task_id="bigeye__subscription_platform_derived__stripe_customers_history__v1",
        table_id="moz-fx-data-shared-prod.subscription_platform_derived.stripe_customers_history_v1",
        warehouse_id="1939",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
    )

    bigeye__subscription_platform_derived__stripe_customers_revised_changelog__v1 = bigquery_bigeye_check(
        task_id="bigeye__subscription_platform_derived__stripe_customers_revised_changelog__v1",
        table_id="moz-fx-data-shared-prod.subscription_platform_derived.stripe_customers_revised_changelog_v1",
        warehouse_id="1939",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
    )

    bigeye__subscription_platform_derived__stripe_logical_subscriptions_attribution__v1 = bigquery_bigeye_check(
        task_id="bigeye__subscription_platform_derived__stripe_logical_subscriptions_attribution__v1",
        table_id="moz-fx-data-shared-prod.subscription_platform_derived.stripe_logical_subscriptions_attribution_v1",
        warehouse_id="1939",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
    )

    bigeye__subscription_platform_derived__stripe_logical_subscriptions_attribution__v2 = bigquery_bigeye_check(
        task_id="bigeye__subscription_platform_derived__stripe_logical_subscriptions_attribution__v2",
        table_id="moz-fx-data-shared-prod.subscription_platform_derived.stripe_logical_subscriptions_attribution_v2",
        warehouse_id="1939",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
    )

    bigeye__subscription_platform_derived__stripe_service_subscriptions_attribution__v1 = bigquery_bigeye_check(
        task_id="bigeye__subscription_platform_derived__stripe_service_subscriptions_attribution__v1",
        table_id="moz-fx-data-shared-prod.subscription_platform_derived.stripe_service_subscriptions_attribution_v1",
        warehouse_id="1939",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
    )

    bigeye__subscription_platform_derived__stripe_service_subscriptions_attribution__v2 = bigquery_bigeye_check(
        task_id="bigeye__subscription_platform_derived__stripe_service_subscriptions_attribution__v2",
        table_id="moz-fx-data-shared-prod.subscription_platform_derived.stripe_service_subscriptions_attribution_v2",
        warehouse_id="1939",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
    )

    bigeye__subscription_platform_derived__stripe_subscriptions_history__v2 = bigquery_bigeye_check(
        task_id="bigeye__subscription_platform_derived__stripe_subscriptions_history__v2",
        table_id="moz-fx-data-shared-prod.subscription_platform_derived.stripe_subscriptions_history_v2",
        warehouse_id="1939",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
    )

    bigeye__subscription_platform_derived__stripe_subscriptions_revised_changelog__v1 = bigquery_bigeye_check(
        task_id="bigeye__subscription_platform_derived__stripe_subscriptions_revised_changelog__v1",
        table_id="moz-fx-data-shared-prod.subscription_platform_derived.stripe_subscriptions_revised_changelog_v1",
        warehouse_id="1939",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        execution_timeout=datetime.timedelta(hours=1),
        retries=1,
    )

    firefox_accounts_derived__recent_fxa_gcp_stderr_events__v1 = bigquery_etl_query(
        task_id="firefox_accounts_derived__recent_fxa_gcp_stderr_events__v1",
        destination_table="recent_fxa_gcp_stderr_events_v1",
        dataset_id="firefox_accounts_derived",
        project_id="moz-fx-data-shared-prod",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    firefox_accounts_derived__recent_fxa_gcp_stdout_events__v1 = bigquery_etl_query(
        task_id="firefox_accounts_derived__recent_fxa_gcp_stdout_events__v1",
        destination_table="recent_fxa_gcp_stdout_events_v1",
        dataset_id="firefox_accounts_derived",
        project_id="moz-fx-data-shared-prod",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    stripe_external__card__v1 = bigquery_etl_query(
        task_id="stripe_external__card__v1",
        destination_table="card_v1",
        dataset_id="stripe_external",
        project_id="moz-fx-data-shared-prod",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    with TaskGroup(
        "stripe_external__card__v1_external",
    ) as stripe_external__card__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_subplat__wait_for_stripe_external__card__v1",
            external_dag_id="bqetl_subplat",
            external_task_id="wait_for_stripe_external__card__v1",
            execution_date="{{ (execution_date - macros.timedelta(seconds=82800)).isoformat() }}",
        )

        stripe_external__card__v1_external.set_upstream(stripe_external__card__v1)

    stripe_external__charge__v1 = bigquery_etl_query(
        task_id="stripe_external__charge__v1",
        destination_table="charge_v1",
        dataset_id="stripe_external",
        project_id="moz-fx-data-shared-prod",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    with TaskGroup(
        "stripe_external__charge__v1_external",
    ) as stripe_external__charge__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_subplat__wait_for_stripe_external__charge__v1",
            external_dag_id="bqetl_subplat",
            external_task_id="wait_for_stripe_external__charge__v1",
            execution_date="{{ (execution_date - macros.timedelta(seconds=82800)).isoformat() }}",
        )

        stripe_external__charge__v1_external.set_upstream(stripe_external__charge__v1)

    stripe_external__coupon__v1 = bigquery_etl_query(
        task_id="stripe_external__coupon__v1",
        destination_table="coupon_v1",
        dataset_id="stripe_external",
        project_id="moz-fx-data-shared-prod",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    with TaskGroup(
        "stripe_external__coupon__v1_external",
    ) as stripe_external__coupon__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_subplat__wait_for_stripe_external__coupon__v1",
            external_dag_id="bqetl_subplat",
            external_task_id="wait_for_stripe_external__coupon__v1",
            execution_date="{{ (execution_date - macros.timedelta(seconds=82800)).isoformat() }}",
        )

        stripe_external__coupon__v1_external.set_upstream(stripe_external__coupon__v1)

    stripe_external__customer__v1 = bigquery_etl_query(
        task_id="stripe_external__customer__v1",
        destination_table="customer_v1",
        dataset_id="stripe_external",
        project_id="moz-fx-data-shared-prod",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    with TaskGroup(
        "stripe_external__customer__v1_external",
    ) as stripe_external__customer__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_subplat__wait_for_stripe_external__customer__v1",
            external_dag_id="bqetl_subplat",
            external_task_id="wait_for_stripe_external__customer__v1",
            execution_date="{{ (execution_date - macros.timedelta(seconds=82800)).isoformat() }}",
        )

        stripe_external__customer__v1_external.set_upstream(
            stripe_external__customer__v1
        )

    stripe_external__customer_discount__v1 = bigquery_etl_query(
        task_id="stripe_external__customer_discount__v1",
        destination_table="customer_discount_v1",
        dataset_id="stripe_external",
        project_id="moz-fx-data-shared-prod",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    stripe_external__customer_discount__v2 = bigquery_etl_query(
        task_id="stripe_external__customer_discount__v2",
        destination_table="customer_discount_v2",
        dataset_id="stripe_external",
        project_id="moz-fx-data-shared-prod",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    stripe_external__customers_changelog__v1 = bigquery_etl_query(
        task_id="stripe_external__customers_changelog__v1",
        destination_table="customers_changelog_v1",
        dataset_id="stripe_external",
        project_id="moz-fx-data-shared-prod",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
        arguments=["--append_table", "--noreplace"],
    )

    stripe_external__discount__v1 = bigquery_etl_query(
        task_id="stripe_external__discount__v1",
        destination_table="discount_v1",
        dataset_id="stripe_external",
        project_id="moz-fx-data-shared-prod",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    stripe_external__invoice__v1 = bigquery_etl_query(
        task_id="stripe_external__invoice__v1",
        destination_table="invoice_v1",
        dataset_id="stripe_external",
        project_id="moz-fx-data-shared-prod",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    with TaskGroup(
        "stripe_external__invoice__v1_external",
    ) as stripe_external__invoice__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_subplat__wait_for_stripe_external__invoice__v1",
            external_dag_id="bqetl_subplat",
            external_task_id="wait_for_stripe_external__invoice__v1",
            execution_date="{{ (execution_date - macros.timedelta(seconds=82800)).isoformat() }}",
        )

        stripe_external__invoice__v1_external.set_upstream(stripe_external__invoice__v1)

    stripe_external__invoice_discount__v1 = bigquery_etl_query(
        task_id="stripe_external__invoice_discount__v1",
        destination_table="invoice_discount_v1",
        dataset_id="stripe_external",
        project_id="moz-fx-data-shared-prod",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    stripe_external__invoice_discount__v2 = bigquery_etl_query(
        task_id="stripe_external__invoice_discount__v2",
        destination_table="invoice_discount_v2",
        dataset_id="stripe_external",
        project_id="moz-fx-data-shared-prod",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    with TaskGroup(
        "stripe_external__invoice_discount__v2_external",
    ) as stripe_external__invoice_discount__v2_external:
        ExternalTaskMarker(
            task_id="bqetl_subplat__wait_for_stripe_external__invoice_discount__v2",
            external_dag_id="bqetl_subplat",
            external_task_id="wait_for_stripe_external__invoice_discount__v2",
            execution_date="{{ (execution_date - macros.timedelta(seconds=82800)).isoformat() }}",
        )

        stripe_external__invoice_discount__v2_external.set_upstream(
            stripe_external__invoice_discount__v2
        )

    stripe_external__invoice_line_item__v1 = bigquery_etl_query(
        task_id="stripe_external__invoice_line_item__v1",
        destination_table="invoice_line_item_v1",
        dataset_id="stripe_external",
        project_id="moz-fx-data-shared-prod",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    stripe_external__plan__v1 = bigquery_etl_query(
        task_id="stripe_external__plan__v1",
        destination_table="plan_v1",
        dataset_id="stripe_external",
        project_id="moz-fx-data-shared-prod",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    with TaskGroup(
        "stripe_external__plan__v1_external",
    ) as stripe_external__plan__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_subplat__wait_for_stripe_external__plan__v1",
            external_dag_id="bqetl_subplat",
            external_task_id="wait_for_stripe_external__plan__v1",
            execution_date="{{ (execution_date - macros.timedelta(seconds=82800)).isoformat() }}",
        )

        stripe_external__plan__v1_external.set_upstream(stripe_external__plan__v1)

    stripe_external__product__v1 = bigquery_etl_query(
        task_id="stripe_external__product__v1",
        destination_table="product_v1",
        dataset_id="stripe_external",
        project_id="moz-fx-data-shared-prod",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    with TaskGroup(
        "stripe_external__product__v1_external",
    ) as stripe_external__product__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_subplat__wait_for_stripe_external__product__v1",
            external_dag_id="bqetl_subplat",
            external_task_id="wait_for_stripe_external__product__v1",
            execution_date="{{ (execution_date - macros.timedelta(seconds=82800)).isoformat() }}",
        )

        stripe_external__product__v1_external.set_upstream(stripe_external__product__v1)

    stripe_external__promotion_code__v1 = bigquery_etl_query(
        task_id="stripe_external__promotion_code__v1",
        destination_table="promotion_code_v1",
        dataset_id="stripe_external",
        project_id="moz-fx-data-shared-prod",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    with TaskGroup(
        "stripe_external__promotion_code__v1_external",
    ) as stripe_external__promotion_code__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_subplat__wait_for_stripe_external__promotion_code__v1",
            external_dag_id="bqetl_subplat",
            external_task_id="wait_for_stripe_external__promotion_code__v1",
            execution_date="{{ (execution_date - macros.timedelta(seconds=82800)).isoformat() }}",
        )

        stripe_external__promotion_code__v1_external.set_upstream(
            stripe_external__promotion_code__v1
        )

    stripe_external__refund__v1 = bigquery_etl_query(
        task_id="stripe_external__refund__v1",
        destination_table="refund_v1",
        dataset_id="stripe_external",
        project_id="moz-fx-data-shared-prod",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    with TaskGroup(
        "stripe_external__refund__v1_external",
    ) as stripe_external__refund__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_subplat__wait_for_stripe_external__refund__v1",
            external_dag_id="bqetl_subplat",
            external_task_id="wait_for_stripe_external__refund__v1",
            execution_date="{{ (execution_date - macros.timedelta(seconds=82800)).isoformat() }}",
        )

        stripe_external__refund__v1_external.set_upstream(stripe_external__refund__v1)

    stripe_external__subscription_discount__v1 = bigquery_etl_query(
        task_id="stripe_external__subscription_discount__v1",
        destination_table="subscription_discount_v1",
        dataset_id="stripe_external",
        project_id="moz-fx-data-shared-prod",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    stripe_external__subscription_discount__v2 = bigquery_etl_query(
        task_id="stripe_external__subscription_discount__v2",
        destination_table="subscription_discount_v2",
        dataset_id="stripe_external",
        project_id="moz-fx-data-shared-prod",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    stripe_external__subscription_history__v1 = bigquery_etl_query(
        task_id="stripe_external__subscription_history__v1",
        destination_table="subscription_history_v1",
        dataset_id="stripe_external",
        project_id="moz-fx-data-shared-prod",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    with TaskGroup(
        "stripe_external__subscription_history__v1_external",
    ) as stripe_external__subscription_history__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_subplat__wait_for_stripe_external__subscription_history__v1",
            external_dag_id="bqetl_subplat",
            external_task_id="wait_for_stripe_external__subscription_history__v1",
            execution_date="{{ (execution_date - macros.timedelta(seconds=82800)).isoformat() }}",
        )

        stripe_external__subscription_history__v1_external.set_upstream(
            stripe_external__subscription_history__v1
        )

    stripe_external__subscription_item__v1 = bigquery_etl_query(
        task_id="stripe_external__subscription_item__v1",
        destination_table="subscription_item_v1",
        dataset_id="stripe_external",
        project_id="moz-fx-data-shared-prod",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    with TaskGroup(
        "stripe_external__subscription_item__v1_external",
    ) as stripe_external__subscription_item__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_subplat__wait_for_stripe_external__subscription_item__v1",
            external_dag_id="bqetl_subplat",
            external_task_id="wait_for_stripe_external__subscription_item__v1",
            execution_date="{{ (execution_date - macros.timedelta(seconds=82800)).isoformat() }}",
        )

        stripe_external__subscription_item__v1_external.set_upstream(
            stripe_external__subscription_item__v1
        )

    stripe_external__subscription_tax_rate__v1 = bigquery_etl_query(
        task_id="stripe_external__subscription_tax_rate__v1",
        destination_table="subscription_tax_rate_v1",
        dataset_id="stripe_external",
        project_id="moz-fx-data-shared-prod",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    stripe_external__subscriptions_changelog__v1 = bigquery_etl_query(
        task_id="stripe_external__subscriptions_changelog__v1",
        destination_table="subscriptions_changelog_v1",
        dataset_id="stripe_external",
        project_id="moz-fx-data-shared-prod",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
        arguments=["--append_table", "--noreplace"],
    )

    stripe_external__tax_rate__v1 = bigquery_etl_query(
        task_id="stripe_external__tax_rate__v1",
        destination_table="tax_rate_v1",
        dataset_id="stripe_external",
        project_id="moz-fx-data-shared-prod",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    subscription_platform_derived__apple_logical_subscriptions_history__v1 = bigquery_etl_query(
        task_id="subscription_platform_derived__apple_logical_subscriptions_history__v1",
        destination_table="apple_logical_subscriptions_history_v1",
        dataset_id="subscription_platform_derived",
        project_id="moz-fx-data-shared-prod",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    subscription_platform_derived__apple_subscriptions__v2 = bigquery_etl_query(
        task_id="subscription_platform_derived__apple_subscriptions__v2",
        destination_table="apple_subscriptions_v2",
        dataset_id="subscription_platform_derived",
        project_id="moz-fx-data-shared-prod",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    subscription_platform_derived__apple_subscriptions_changelog__v1 = (
        bigquery_etl_query(
            task_id="subscription_platform_derived__apple_subscriptions_changelog__v1",
            destination_table="apple_subscriptions_changelog_v1",
            dataset_id="subscription_platform_derived",
            project_id="moz-fx-data-shared-prod",
            owner="srose@mozilla.com",
            email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
            date_partition_parameter=None,
            depends_on_past=False,
            task_concurrency=1,
            arguments=["--append_table", "--noreplace"],
        )
    )

    subscription_platform_derived__apple_subscriptions_history__v1 = bigquery_etl_query(
        task_id="subscription_platform_derived__apple_subscriptions_history__v1",
        destination_table="apple_subscriptions_history_v1",
        dataset_id="subscription_platform_derived",
        project_id="moz-fx-data-shared-prod",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    subscription_platform_derived__apple_subscriptions_revised_changelog__v1 = bigquery_etl_query(
        task_id="subscription_platform_derived__apple_subscriptions_revised_changelog__v1",
        destination_table="apple_subscriptions_revised_changelog_v1",
        dataset_id="subscription_platform_derived",
        project_id="moz-fx-data-shared-prod",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
        arguments=["--append_table", "--noreplace"],
    )

    subscription_platform_derived__firestore_stripe_subscriptions_status__v1 = bigquery_etl_query(
        task_id="subscription_platform_derived__firestore_stripe_subscriptions_status__v1",
        destination_table="firestore_stripe_subscriptions_status_v1",
        dataset_id="subscription_platform_derived",
        project_id="moz-fx-data-shared-prod",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="date",
        depends_on_past=False,
    )

    subscription_platform_derived__google_logical_subscriptions_history__v1 = bigquery_etl_query(
        task_id="subscription_platform_derived__google_logical_subscriptions_history__v1",
        destination_table="google_logical_subscriptions_history_v1",
        dataset_id="subscription_platform_derived",
        project_id="moz-fx-data-shared-prod",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    subscription_platform_derived__google_subscriptions__v2 = bigquery_etl_query(
        task_id="subscription_platform_derived__google_subscriptions__v2",
        destination_table="google_subscriptions_v2",
        dataset_id="subscription_platform_derived",
        project_id="moz-fx-data-shared-prod",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    subscription_platform_derived__google_subscriptions_changelog__v1 = (
        bigquery_etl_query(
            task_id="subscription_platform_derived__google_subscriptions_changelog__v1",
            destination_table="google_subscriptions_changelog_v1",
            dataset_id="subscription_platform_derived",
            project_id="moz-fx-data-shared-prod",
            owner="srose@mozilla.com",
            email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
            date_partition_parameter=None,
            depends_on_past=False,
            task_concurrency=1,
            arguments=["--append_table", "--noreplace"],
        )
    )

    subscription_platform_derived__google_subscriptions_history__v1 = (
        bigquery_etl_query(
            task_id="subscription_platform_derived__google_subscriptions_history__v1",
            destination_table="google_subscriptions_history_v1",
            dataset_id="subscription_platform_derived",
            project_id="moz-fx-data-shared-prod",
            owner="srose@mozilla.com",
            email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
            date_partition_parameter=None,
            depends_on_past=False,
            task_concurrency=1,
        )
    )

    subscription_platform_derived__google_subscriptions_revised_changelog__v1 = bigquery_etl_query(
        task_id="subscription_platform_derived__google_subscriptions_revised_changelog__v1",
        destination_table="google_subscriptions_revised_changelog_v1",
        dataset_id="subscription_platform_derived",
        project_id="moz-fx-data-shared-prod",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
        arguments=["--append_table", "--noreplace"],
    )

    subscription_platform_derived__logical_subscriptions__v1 = bigquery_etl_query(
        task_id="subscription_platform_derived__logical_subscriptions__v1",
        destination_table="logical_subscriptions_v1",
        dataset_id="subscription_platform_derived",
        project_id="moz-fx-data-shared-prod",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    subscription_platform_derived__logical_subscriptions_history__v1 = (
        bigquery_etl_query(
            task_id="subscription_platform_derived__logical_subscriptions_history__v1",
            destination_table="logical_subscriptions_history_v1",
            dataset_id="subscription_platform_derived",
            project_id="moz-fx-data-shared-prod",
            owner="srose@mozilla.com",
            email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
            date_partition_parameter=None,
            depends_on_past=False,
            task_concurrency=1,
        )
    )

    with TaskGroup(
        "subscription_platform_derived__logical_subscriptions_history__v1_external",
    ) as subscription_platform_derived__logical_subscriptions_history__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_subplat__wait_for_subscription_platform_derived__logical_subscriptions_history__v1",
            external_dag_id="bqetl_subplat",
            external_task_id="wait_for_subscription_platform_derived__logical_subscriptions_history__v1",
            execution_date="{{ (execution_date - macros.timedelta(seconds=82800)).isoformat() }}",
        )

        subscription_platform_derived__logical_subscriptions_history__v1_external.set_upstream(
            subscription_platform_derived__logical_subscriptions_history__v1
        )

    subscription_platform_derived__recent_daily_active_logical_subscriptions__v1 = bigquery_etl_query(
        task_id="subscription_platform_derived__recent_daily_active_logical_subscriptions__v1",
        destination_table="recent_daily_active_logical_subscriptions_v1",
        dataset_id="subscription_platform_derived",
        project_id="moz-fx-data-shared-prod",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    subscription_platform_derived__recent_daily_active_service_subscriptions__v1 = bigquery_etl_query(
        task_id="subscription_platform_derived__recent_daily_active_service_subscriptions__v1",
        destination_table="recent_daily_active_service_subscriptions_v1",
        dataset_id="subscription_platform_derived",
        project_id="moz-fx-data-shared-prod",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    subscription_platform_derived__recent_logical_subscription_events__v1 = bigquery_etl_query(
        task_id="subscription_platform_derived__recent_logical_subscription_events__v1",
        destination_table="recent_logical_subscription_events_v1",
        dataset_id="subscription_platform_derived",
        project_id="moz-fx-data-shared-prod",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    subscription_platform_derived__recent_monthly_active_logical_subscriptions__v1 = bigquery_etl_query(
        task_id="subscription_platform_derived__recent_monthly_active_logical_subscriptions__v1",
        destination_table="recent_monthly_active_logical_subscriptions_v1",
        dataset_id="subscription_platform_derived",
        project_id="moz-fx-data-shared-prod",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    subscription_platform_derived__recent_monthly_active_service_subscriptions__v1 = bigquery_etl_query(
        task_id="subscription_platform_derived__recent_monthly_active_service_subscriptions__v1",
        destination_table="recent_monthly_active_service_subscriptions_v1",
        dataset_id="subscription_platform_derived",
        project_id="moz-fx-data-shared-prod",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    subscription_platform_derived__recent_service_subscription_events__v1 = bigquery_etl_query(
        task_id="subscription_platform_derived__recent_service_subscription_events__v1",
        destination_table="recent_service_subscription_events_v1",
        dataset_id="subscription_platform_derived",
        project_id="moz-fx-data-shared-prod",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    subscription_platform_derived__recent_subplat_attribution_impressions__v1 = bigquery_etl_query(
        task_id="subscription_platform_derived__recent_subplat_attribution_impressions__v1",
        destination_table="recent_subplat_attribution_impressions_v1",
        dataset_id="subscription_platform_derived",
        project_id="moz-fx-data-shared-prod",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    subscription_platform_derived__service_subscriptions__v1 = bigquery_etl_query(
        task_id="subscription_platform_derived__service_subscriptions__v1",
        destination_table="service_subscriptions_v1",
        dataset_id="subscription_platform_derived",
        project_id="moz-fx-data-shared-prod",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    subscription_platform_derived__service_subscriptions_history__v1 = (
        bigquery_etl_query(
            task_id="subscription_platform_derived__service_subscriptions_history__v1",
            destination_table="service_subscriptions_history_v1",
            dataset_id="subscription_platform_derived",
            project_id="moz-fx-data-shared-prod",
            owner="srose@mozilla.com",
            email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
            date_partition_parameter=None,
            depends_on_past=False,
            task_concurrency=1,
        )
    )

    with TaskGroup(
        "subscription_platform_derived__service_subscriptions_history__v1_external",
    ) as subscription_platform_derived__service_subscriptions_history__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_subplat__wait_for_subscription_platform_derived__service_subscriptions_history__v1",
            external_dag_id="bqetl_subplat",
            external_task_id="wait_for_subscription_platform_derived__service_subscriptions_history__v1",
            execution_date="{{ (execution_date - macros.timedelta(seconds=82800)).isoformat() }}",
        )

        subscription_platform_derived__service_subscriptions_history__v1_external.set_upstream(
            subscription_platform_derived__service_subscriptions_history__v1
        )

    subscription_platform_derived__services__v1 = bigquery_etl_query(
        task_id="subscription_platform_derived__services__v1",
        destination_table="services_v1",
        dataset_id="subscription_platform_derived",
        project_id="moz-fx-data-shared-prod",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    with TaskGroup(
        "subscription_platform_derived__services__v1_external",
    ) as subscription_platform_derived__services__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_subplat__wait_for_subscription_platform_derived__services__v1",
            external_dag_id="bqetl_subplat",
            external_task_id="wait_for_subscription_platform_derived__services__v1",
            execution_date="{{ (execution_date - macros.timedelta(seconds=82800)).isoformat() }}",
        )

        subscription_platform_derived__services__v1_external.set_upstream(
            subscription_platform_derived__services__v1
        )

    subscription_platform_derived__stripe_customers_history__v1 = bigquery_etl_query(
        task_id="subscription_platform_derived__stripe_customers_history__v1",
        destination_table="stripe_customers_history_v1",
        dataset_id="subscription_platform_derived",
        project_id="moz-fx-data-shared-prod",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    subscription_platform_derived__stripe_customers_revised_changelog__v1 = bigquery_etl_query(
        task_id="subscription_platform_derived__stripe_customers_revised_changelog__v1",
        destination_table="stripe_customers_revised_changelog_v1",
        dataset_id="subscription_platform_derived",
        project_id="moz-fx-data-shared-prod",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="date",
        depends_on_past=False,
    )

    subscription_platform_derived__stripe_logical_subscriptions_attribution__v1 = bigquery_etl_query(
        task_id="subscription_platform_derived__stripe_logical_subscriptions_attribution__v1",
        destination_table="stripe_logical_subscriptions_attribution_v1",
        dataset_id="subscription_platform_derived",
        project_id="moz-fx-data-shared-prod",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="date",
        depends_on_past=False,
    )

    subscription_platform_derived__stripe_logical_subscriptions_attribution__v2 = bigquery_etl_query(
        task_id="subscription_platform_derived__stripe_logical_subscriptions_attribution__v2",
        destination_table="stripe_logical_subscriptions_attribution_v2",
        dataset_id="subscription_platform_derived",
        project_id="moz-fx-data-shared-prod",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="date",
        depends_on_past=False,
    )

    subscription_platform_derived__stripe_logical_subscriptions_history__v1 = bigquery_etl_query(
        task_id="subscription_platform_derived__stripe_logical_subscriptions_history__v1",
        destination_table="stripe_logical_subscriptions_history_v1",
        dataset_id="subscription_platform_derived",
        project_id="moz-fx-data-shared-prod",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    subscription_platform_derived__stripe_plans__v1 = bigquery_etl_query(
        task_id="subscription_platform_derived__stripe_plans__v1",
        destination_table="stripe_plans_v1",
        dataset_id="subscription_platform_derived",
        project_id="moz-fx-data-shared-prod",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    subscription_platform_derived__stripe_products__v1 = bigquery_etl_query(
        task_id="subscription_platform_derived__stripe_products__v1",
        destination_table="stripe_products_v1",
        dataset_id="subscription_platform_derived",
        project_id="moz-fx-data-shared-prod",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    subscription_platform_derived__stripe_service_subscriptions_attribution__v1 = bigquery_etl_query(
        task_id="subscription_platform_derived__stripe_service_subscriptions_attribution__v1",
        destination_table="stripe_service_subscriptions_attribution_v1",
        dataset_id="subscription_platform_derived",
        project_id="moz-fx-data-shared-prod",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="date",
        depends_on_past=False,
    )

    subscription_platform_derived__stripe_service_subscriptions_attribution__v2 = bigquery_etl_query(
        task_id="subscription_platform_derived__stripe_service_subscriptions_attribution__v2",
        destination_table="stripe_service_subscriptions_attribution_v2",
        dataset_id="subscription_platform_derived",
        project_id="moz-fx-data-shared-prod",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="date",
        depends_on_past=False,
    )

    subscription_platform_derived__stripe_subscriptions__v2 = bigquery_etl_query(
        task_id="subscription_platform_derived__stripe_subscriptions__v2",
        destination_table="stripe_subscriptions_v2",
        dataset_id="subscription_platform_derived",
        project_id="moz-fx-data-shared-prod",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    subscription_platform_derived__stripe_subscriptions_history__v2 = (
        bigquery_etl_query(
            task_id="subscription_platform_derived__stripe_subscriptions_history__v2",
            destination_table="stripe_subscriptions_history_v2",
            dataset_id="subscription_platform_derived",
            project_id="moz-fx-data-shared-prod",
            owner="srose@mozilla.com",
            email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
            date_partition_parameter=None,
            depends_on_past=False,
            task_concurrency=1,
        )
    )

    subscription_platform_derived__stripe_subscriptions_revised_changelog__v1 = bigquery_etl_query(
        task_id="subscription_platform_derived__stripe_subscriptions_revised_changelog__v1",
        destination_table="stripe_subscriptions_revised_changelog_v1",
        dataset_id="subscription_platform_derived",
        project_id="moz-fx-data-shared-prod",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="date",
        depends_on_past=False,
    )

    subscription_platform_derived__subplat_flow_events__v1 = bigquery_etl_query(
        task_id="subscription_platform_derived__subplat_flow_events__v1",
        destination_table="subplat_flow_events_v1",
        dataset_id="subscription_platform_derived",
        project_id="moz-fx-data-shared-prod",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="date",
        depends_on_past=False,
    )

    with TaskGroup(
        "subscription_platform_derived__subplat_flow_events__v1_external",
    ) as subscription_platform_derived__subplat_flow_events__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_subplat__wait_for_subscription_platform_derived__subplat_flow_events__v1",
            external_dag_id="bqetl_subplat",
            external_task_id="wait_for_subscription_platform_derived__subplat_flow_events__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=79200)).isoformat() }}",
        )

        subscription_platform_derived__subplat_flow_events__v1_external.set_upstream(
            subscription_platform_derived__subplat_flow_events__v1
        )

    bigeye__subscription_platform_derived__apple_logical_subscriptions_history__v1.set_upstream(
        subscription_platform_derived__apple_logical_subscriptions_history__v1
    )

    bigeye__subscription_platform_derived__apple_subscriptions_changelog__v1.set_upstream(
        subscription_platform_derived__apple_subscriptions_changelog__v1
    )

    bigeye__subscription_platform_derived__apple_subscriptions_history__v1.set_upstream(
        subscription_platform_derived__apple_subscriptions_history__v1
    )

    bigeye__subscription_platform_derived__apple_subscriptions_revised_changelog__v1.set_upstream(
        subscription_platform_derived__apple_subscriptions_revised_changelog__v1
    )

    bigeye__subscription_platform_derived__google_logical_subscriptions_history__v1.set_upstream(
        subscription_platform_derived__google_logical_subscriptions_history__v1
    )

    bigeye__subscription_platform_derived__google_subscriptions_changelog__v1.set_upstream(
        subscription_platform_derived__google_subscriptions_changelog__v1
    )

    bigeye__subscription_platform_derived__google_subscriptions_history__v1.set_upstream(
        subscription_platform_derived__google_subscriptions_history__v1
    )

    bigeye__subscription_platform_derived__google_subscriptions_revised_changelog__v1.set_upstream(
        subscription_platform_derived__google_subscriptions_revised_changelog__v1
    )

    bigeye__subscription_platform_derived__stripe_customers_history__v1.set_upstream(
        subscription_platform_derived__stripe_customers_history__v1
    )

    bigeye__subscription_platform_derived__stripe_customers_revised_changelog__v1.set_upstream(
        subscription_platform_derived__stripe_customers_revised_changelog__v1
    )

    bigeye__subscription_platform_derived__stripe_logical_subscriptions_attribution__v1.set_upstream(
        subscription_platform_derived__stripe_logical_subscriptions_attribution__v1
    )

    bigeye__subscription_platform_derived__stripe_logical_subscriptions_attribution__v2.set_upstream(
        subscription_platform_derived__stripe_logical_subscriptions_attribution__v2
    )

    bigeye__subscription_platform_derived__stripe_service_subscriptions_attribution__v1.set_upstream(
        subscription_platform_derived__stripe_service_subscriptions_attribution__v1
    )

    bigeye__subscription_platform_derived__stripe_service_subscriptions_attribution__v2.set_upstream(
        subscription_platform_derived__stripe_service_subscriptions_attribution__v2
    )

    bigeye__subscription_platform_derived__stripe_subscriptions_history__v2.set_upstream(
        subscription_platform_derived__stripe_subscriptions_history__v2
    )

    bigeye__subscription_platform_derived__stripe_subscriptions_revised_changelog__v1.set_upstream(
        subscription_platform_derived__stripe_subscriptions_revised_changelog__v1
    )

    stripe_external__card__v1.set_upstream(fivetran_stripe_sync_start)

    stripe_external__charge__v1.set_upstream(fivetran_stripe_sync_start)

    stripe_external__coupon__v1.set_upstream(fivetran_stripe_sync_start)

    stripe_external__customer__v1.set_upstream(fivetran_stripe_sync_start)

    stripe_external__customer_discount__v1.set_upstream(fivetran_stripe_sync_start)

    stripe_external__customer_discount__v2.set_upstream(stripe_external__customer__v1)

    stripe_external__customer_discount__v2.set_upstream(
        stripe_external__customer_discount__v1
    )

    stripe_external__customer_discount__v2.set_upstream(stripe_external__discount__v1)

    stripe_external__customers_changelog__v1.set_upstream(stripe_external__coupon__v1)

    stripe_external__customers_changelog__v1.set_upstream(stripe_external__customer__v1)

    stripe_external__customers_changelog__v1.set_upstream(
        stripe_external__customer_discount__v2
    )

    stripe_external__discount__v1.set_upstream(fivetran_stripe_sync_start)

    stripe_external__invoice__v1.set_upstream(fivetran_stripe_sync_start)

    stripe_external__invoice_discount__v1.set_upstream(fivetran_stripe_sync_start)

    stripe_external__invoice_discount__v2.set_upstream(stripe_external__discount__v1)

    stripe_external__invoice_discount__v2.set_upstream(stripe_external__invoice__v1)

    stripe_external__invoice_discount__v2.set_upstream(
        stripe_external__invoice_discount__v1
    )

    stripe_external__invoice_line_item__v1.set_upstream(fivetran_stripe_sync_start)

    stripe_external__plan__v1.set_upstream(fivetran_stripe_sync_start)

    stripe_external__product__v1.set_upstream(fivetran_stripe_sync_start)

    stripe_external__promotion_code__v1.set_upstream(fivetran_stripe_sync_start)

    stripe_external__refund__v1.set_upstream(fivetran_stripe_sync_start)

    stripe_external__subscription_discount__v1.set_upstream(fivetran_stripe_sync_start)

    stripe_external__subscription_discount__v2.set_upstream(
        stripe_external__discount__v1
    )

    stripe_external__subscription_discount__v2.set_upstream(
        stripe_external__subscription_discount__v1
    )

    stripe_external__subscription_discount__v2.set_upstream(
        stripe_external__subscription_history__v1
    )

    stripe_external__subscription_history__v1.set_upstream(fivetran_stripe_sync_start)

    stripe_external__subscription_item__v1.set_upstream(fivetran_stripe_sync_start)

    stripe_external__subscription_tax_rate__v1.set_upstream(fivetran_stripe_sync_start)

    stripe_external__subscriptions_changelog__v1.set_upstream(
        stripe_external__coupon__v1
    )

    stripe_external__subscriptions_changelog__v1.set_upstream(stripe_external__plan__v1)

    stripe_external__subscriptions_changelog__v1.set_upstream(
        stripe_external__product__v1
    )

    stripe_external__subscriptions_changelog__v1.set_upstream(
        stripe_external__subscription_discount__v2
    )

    stripe_external__subscriptions_changelog__v1.set_upstream(
        stripe_external__subscription_history__v1
    )

    stripe_external__subscriptions_changelog__v1.set_upstream(
        stripe_external__subscription_item__v1
    )

    stripe_external__subscriptions_changelog__v1.set_upstream(
        stripe_external__subscription_tax_rate__v1
    )

    stripe_external__subscriptions_changelog__v1.set_upstream(
        stripe_external__tax_rate__v1
    )

    stripe_external__tax_rate__v1.set_upstream(fivetran_stripe_sync_start)

    subscription_platform_derived__apple_logical_subscriptions_history__v1.set_upstream(
        bigeye__subscription_platform_derived__apple_subscriptions_history__v1
    )

    subscription_platform_derived__apple_logical_subscriptions_history__v1.set_upstream(
        subscription_platform_derived__services__v1
    )

    subscription_platform_derived__apple_logical_subscriptions_history__v1.set_upstream(
        subscription_platform_derived__stripe_plans__v1
    )

    subscription_platform_derived__apple_logical_subscriptions_history__v1.set_upstream(
        subscription_platform_derived__stripe_products__v1
    )

    subscription_platform_derived__apple_subscriptions__v2.set_upstream(
        bigeye__subscription_platform_derived__apple_subscriptions_history__v1
    )

    subscription_platform_derived__apple_subscriptions_history__v1.set_upstream(
        bigeye__subscription_platform_derived__apple_subscriptions_revised_changelog__v1
    )

    subscription_platform_derived__apple_subscriptions_revised_changelog__v1.set_upstream(
        bigeye__subscription_platform_derived__apple_subscriptions_changelog__v1
    )

    subscription_platform_derived__google_logical_subscriptions_history__v1.set_upstream(
        bigeye__subscription_platform_derived__google_subscriptions_history__v1
    )

    subscription_platform_derived__google_logical_subscriptions_history__v1.set_upstream(
        subscription_platform_derived__services__v1
    )

    subscription_platform_derived__google_logical_subscriptions_history__v1.set_upstream(
        subscription_platform_derived__stripe_plans__v1
    )

    subscription_platform_derived__google_logical_subscriptions_history__v1.set_upstream(
        subscription_platform_derived__stripe_products__v1
    )

    subscription_platform_derived__google_subscriptions__v2.set_upstream(
        bigeye__subscription_platform_derived__google_subscriptions_history__v1
    )

    subscription_platform_derived__google_subscriptions_history__v1.set_upstream(
        bigeye__subscription_platform_derived__google_subscriptions_revised_changelog__v1
    )

    subscription_platform_derived__google_subscriptions_revised_changelog__v1.set_upstream(
        bigeye__subscription_platform_derived__google_subscriptions_changelog__v1
    )

    subscription_platform_derived__logical_subscriptions__v1.set_upstream(
        subscription_platform_derived__logical_subscriptions_history__v1
    )

    subscription_platform_derived__logical_subscriptions_history__v1.set_upstream(
        bigeye__subscription_platform_derived__apple_logical_subscriptions_history__v1
    )

    subscription_platform_derived__logical_subscriptions_history__v1.set_upstream(
        bigeye__subscription_platform_derived__google_logical_subscriptions_history__v1
    )

    subscription_platform_derived__logical_subscriptions_history__v1.set_upstream(
        bigeye__subscription_platform_derived__stripe_logical_subscriptions_attribution__v1
    )

    subscription_platform_derived__logical_subscriptions_history__v1.set_upstream(
        bigeye__subscription_platform_derived__stripe_logical_subscriptions_attribution__v2
    )

    subscription_platform_derived__logical_subscriptions_history__v1.set_upstream(
        subscription_platform_derived__stripe_logical_subscriptions_history__v1
    )

    subscription_platform_derived__recent_daily_active_logical_subscriptions__v1.set_upstream(
        subscription_platform_derived__logical_subscriptions_history__v1
    )

    subscription_platform_derived__recent_daily_active_service_subscriptions__v1.set_upstream(
        subscription_platform_derived__service_subscriptions_history__v1
    )

    subscription_platform_derived__recent_logical_subscription_events__v1.set_upstream(
        subscription_platform_derived__logical_subscriptions_history__v1
    )

    subscription_platform_derived__recent_monthly_active_logical_subscriptions__v1.set_upstream(
        subscription_platform_derived__logical_subscriptions_history__v1
    )

    subscription_platform_derived__recent_monthly_active_service_subscriptions__v1.set_upstream(
        subscription_platform_derived__service_subscriptions_history__v1
    )

    subscription_platform_derived__recent_service_subscription_events__v1.set_upstream(
        subscription_platform_derived__service_subscriptions_history__v1
    )

    subscription_platform_derived__recent_subplat_attribution_impressions__v1.set_upstream(
        subscription_platform_derived__services__v1
    )

    subscription_platform_derived__recent_subplat_attribution_impressions__v1.set_upstream(
        subscription_platform_derived__subplat_flow_events__v1
    )

    subscription_platform_derived__service_subscriptions__v1.set_upstream(
        subscription_platform_derived__service_subscriptions_history__v1
    )

    subscription_platform_derived__service_subscriptions_history__v1.set_upstream(
        bigeye__subscription_platform_derived__stripe_service_subscriptions_attribution__v1
    )

    subscription_platform_derived__service_subscriptions_history__v1.set_upstream(
        bigeye__subscription_platform_derived__stripe_service_subscriptions_attribution__v2
    )

    subscription_platform_derived__service_subscriptions_history__v1.set_upstream(
        subscription_platform_derived__logical_subscriptions_history__v1
    )

    subscription_platform_derived__services__v1.set_upstream(
        subscription_platform_derived__stripe_plans__v1
    )

    subscription_platform_derived__services__v1.set_upstream(
        subscription_platform_derived__stripe_products__v1
    )

    subscription_platform_derived__stripe_customers_history__v1.set_upstream(
        bigeye__subscription_platform_derived__stripe_customers_revised_changelog__v1
    )

    subscription_platform_derived__stripe_customers_revised_changelog__v1.set_upstream(
        stripe_external__customers_changelog__v1
    )

    subscription_platform_derived__stripe_customers_revised_changelog__v1.set_upstream(
        stripe_external__subscriptions_changelog__v1
    )

    subscription_platform_derived__stripe_logical_subscriptions_attribution__v1.set_upstream(
        subscription_platform_derived__recent_subplat_attribution_impressions__v1
    )

    subscription_platform_derived__stripe_logical_subscriptions_attribution__v1.set_upstream(
        subscription_platform_derived__stripe_logical_subscriptions_history__v1
    )

    subscription_platform_derived__stripe_logical_subscriptions_attribution__v2.set_upstream(
        bigeye__subscription_platform_derived__stripe_subscriptions_history__v2
    )

    subscription_platform_derived__stripe_logical_subscriptions_attribution__v2.set_upstream(
        subscription_platform_derived__stripe_logical_subscriptions_history__v1
    )

    subscription_platform_derived__stripe_logical_subscriptions_history__v1.set_upstream(
        bigeye__subscription_platform_derived__stripe_subscriptions_history__v2
    )

    subscription_platform_derived__stripe_logical_subscriptions_history__v1.set_upstream(
        stripe_external__card__v1
    )

    subscription_platform_derived__stripe_logical_subscriptions_history__v1.set_upstream(
        stripe_external__charge__v1
    )

    subscription_platform_derived__stripe_logical_subscriptions_history__v1.set_upstream(
        stripe_external__coupon__v1
    )

    subscription_platform_derived__stripe_logical_subscriptions_history__v1.set_upstream(
        stripe_external__invoice__v1
    )

    subscription_platform_derived__stripe_logical_subscriptions_history__v1.set_upstream(
        stripe_external__invoice_discount__v2
    )

    subscription_platform_derived__stripe_logical_subscriptions_history__v1.set_upstream(
        stripe_external__invoice_line_item__v1
    )

    subscription_platform_derived__stripe_logical_subscriptions_history__v1.set_upstream(
        stripe_external__promotion_code__v1
    )

    subscription_platform_derived__stripe_logical_subscriptions_history__v1.set_upstream(
        stripe_external__refund__v1
    )

    subscription_platform_derived__stripe_logical_subscriptions_history__v1.set_upstream(
        subscription_platform_derived__services__v1
    )

    subscription_platform_derived__stripe_plans__v1.set_upstream(
        stripe_external__plan__v1
    )

    subscription_platform_derived__stripe_products__v1.set_upstream(
        stripe_external__product__v1
    )

    subscription_platform_derived__stripe_service_subscriptions_attribution__v1.set_upstream(
        subscription_platform_derived__logical_subscriptions_history__v1
    )

    subscription_platform_derived__stripe_service_subscriptions_attribution__v1.set_upstream(
        subscription_platform_derived__recent_subplat_attribution_impressions__v1
    )

    subscription_platform_derived__stripe_service_subscriptions_attribution__v2.set_upstream(
        bigeye__subscription_platform_derived__stripe_subscriptions_history__v2
    )

    subscription_platform_derived__stripe_service_subscriptions_attribution__v2.set_upstream(
        subscription_platform_derived__logical_subscriptions_history__v1
    )

    subscription_platform_derived__stripe_subscriptions__v2.set_upstream(
        bigeye__subscription_platform_derived__stripe_subscriptions_history__v2
    )

    subscription_platform_derived__stripe_subscriptions_history__v2.set_upstream(
        bigeye__subscription_platform_derived__stripe_customers_history__v1
    )

    subscription_platform_derived__stripe_subscriptions_history__v2.set_upstream(
        bigeye__subscription_platform_derived__stripe_subscriptions_revised_changelog__v1
    )

    subscription_platform_derived__stripe_subscriptions_revised_changelog__v1.set_upstream(
        stripe_external__invoice_line_item__v1
    )

    subscription_platform_derived__stripe_subscriptions_revised_changelog__v1.set_upstream(
        stripe_external__subscriptions_changelog__v1
    )

    subscription_platform_derived__stripe_subscriptions_revised_changelog__v1.set_upstream(
        subscription_platform_derived__stripe_plans__v1
    )

    subscription_platform_derived__stripe_subscriptions_revised_changelog__v1.set_upstream(
        subscription_platform_derived__stripe_products__v1
    )

    subscription_platform_derived__subplat_flow_events__v1.set_upstream(
        firefox_accounts_derived__recent_fxa_gcp_stderr_events__v1
    )

    subscription_platform_derived__subplat_flow_events__v1.set_upstream(
        firefox_accounts_derived__recent_fxa_gcp_stdout_events__v1
    )

    subscription_platform_derived__subplat_flow_events__v1.set_upstream(
        subscription_platform_derived__services__v1
    )
