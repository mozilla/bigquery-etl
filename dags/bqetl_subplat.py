# Generated via https://github.com/mozilla/bigquery-etl/blob/main/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskMarker
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
import datetime
from utils.constants import ALLOWED_STATES, FAILED_STATES
from utils.gcp import bigquery_etl_query, gke_command, bigquery_dq_check

from fivetran_provider.operators.fivetran import FivetranOperator
from fivetran_provider.sensors.fivetran import FivetranSensor
from utils.callbacks import retry_tasks_callback

docs = """
### bqetl_subplat

Built from bigquery-etl repo, [`dags/bqetl_subplat.py`](https://github.com/mozilla/bigquery-etl/blob/main/dags/bqetl_subplat.py)

#### Description

Daily imports for Subscription Platform data from Stripe and the Mozilla VPN
operational DB as well as derived tables based on that data.

Depends on `bqetl_fxa_events`, so is scheduled to run a bit after that.

Stripe data retrieved by stripe_external__itemized_payout_reconciliation__v5
task has highly viariable availability timing, so it is possible for it to
fail with the following type of error:
`Error: Request req_OTssZ0Zv1cEmmm: Data for the report type
        payout_reconciliation.itemized.5 is only available through
        2022-05-08 12:00:00 UTC; you requested `interval_end`
        = 2022-05-09 00:00:00 UTC.`
In such cases the failure is expected, the task will continue to retry every
30 minutes until the data becomes available. If failure observed looks
different then it should be reported using the Airflow triage process.

#### Owner

srose@mozilla.com
"""


default_args = {
    "owner": "srose@mozilla.com",
    "start_date": datetime.datetime(2021, 7, 20, 0, 0),
    "end_date": None,
    "email": ["telemetry-alerts@mozilla.com", "srose@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
}

tags = ["impact/tier_1", "repo/bigquery-etl"]

with DAG(
    "bqetl_subplat",
    default_args=default_args,
    schedule_interval="45 1 * * *",
    doc_md=docs,
    tags=tags,
) as dag:
    cjms_bigquery__flows__v1 = bigquery_etl_query(
        task_id="cjms_bigquery__flows__v1",
        destination_table="flows_v1",
        dataset_id="moz-fx-cjms-prod-f3c7:cjms_bigquery",
        project_id="moz-fx-data-shared-prod",
        sql_file_path="sql/moz-fx-cjms-prod-f3c7/cjms_bigquery/flows_v1/query.sql",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    cjms_bigquery__refunds__v1 = bigquery_etl_query(
        task_id="cjms_bigquery__refunds__v1",
        destination_table="refunds_v1",
        dataset_id="moz-fx-cjms-prod-f3c7:cjms_bigquery",
        project_id="moz-fx-data-shared-prod",
        sql_file_path="sql/moz-fx-cjms-prod-f3c7/cjms_bigquery/refunds_v1/query.sql",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    cjms_bigquery__subscriptions__v1 = bigquery_etl_query(
        task_id="cjms_bigquery__subscriptions__v1",
        destination_table="subscriptions_v1",
        dataset_id="moz-fx-cjms-prod-f3c7:cjms_bigquery",
        project_id="moz-fx-data-shared-prod",
        sql_file_path="sql/moz-fx-cjms-prod-f3c7/cjms_bigquery/subscriptions_v1/query.sql",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    hubs_derived__active_subscription_ids__v1 = bigquery_etl_query(
        task_id="hubs_derived__active_subscription_ids__v1",
        destination_table='active_subscription_ids_v1${{ macros.ds_format(macros.ds_add(ds, -7), "%Y-%m-%d", "%Y%m%d") }}',
        dataset_id="hubs_derived",
        project_id="moz-fx-data-shared-prod",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=True,
        parameters=["date:DATE:{{macros.ds_add(ds, -7)}}"],
    )

    hubs_derived__active_subscriptions__v1 = bigquery_etl_query(
        task_id="hubs_derived__active_subscriptions__v1",
        destination_table='active_subscriptions_v1${{ macros.ds_format(macros.ds_add(ds, -7), "%Y-%m-%d", "%Y%m%d") }}',
        dataset_id="hubs_derived",
        project_id="moz-fx-data-shared-prod",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        parameters=["date:DATE:{{macros.ds_add(ds, -7)}}"],
    )

    hubs_derived__subscription_events__v1 = bigquery_etl_query(
        task_id="hubs_derived__subscription_events__v1",
        destination_table='subscription_events_v1${{ macros.ds_format(macros.ds_add(ds, -8), "%Y-%m-%d", "%Y%m%d") }}',
        dataset_id="hubs_derived",
        project_id="moz-fx-data-shared-prod",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        parameters=["date:DATE:{{macros.ds_add(ds, -8)}}"],
    )

    hubs_derived__subscriptions__v1 = bigquery_etl_query(
        task_id="hubs_derived__subscriptions__v1",
        destination_table="subscriptions_v1",
        dataset_id="hubs_derived",
        project_id="moz-fx-data-shared-prod",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    mozilla_vpn_derived__active_subscription_ids__v1 = bigquery_etl_query(
        task_id="mozilla_vpn_derived__active_subscription_ids__v1",
        destination_table='active_subscription_ids_v1${{ macros.ds_format(macros.ds_add(ds, -7), "%Y-%m-%d", "%Y%m%d") }}',
        dataset_id="mozilla_vpn_derived",
        project_id="moz-fx-data-shared-prod",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=True,
        parameters=["date:DATE:{{macros.ds_add(ds, -7)}}"],
    )

    mozilla_vpn_derived__active_subscriptions__v1 = bigquery_etl_query(
        task_id="mozilla_vpn_derived__active_subscriptions__v1",
        destination_table='active_subscriptions_v1${{ macros.ds_format(macros.ds_add(ds, -7), "%Y-%m-%d", "%Y%m%d") }}',
        dataset_id="mozilla_vpn_derived",
        project_id="moz-fx-data-shared-prod",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        parameters=["date:DATE:{{macros.ds_add(ds, -7)}}"],
    )

    mozilla_vpn_derived__add_device_events__v1 = bigquery_etl_query(
        task_id="mozilla_vpn_derived__add_device_events__v1",
        destination_table="add_device_events_v1",
        dataset_id="mozilla_vpn_derived",
        project_id="moz-fx-data-shared-prod",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    mozilla_vpn_derived__all_subscriptions__v1 = bigquery_etl_query(
        task_id="mozilla_vpn_derived__all_subscriptions__v1",
        destination_table="all_subscriptions_v1",
        dataset_id="mozilla_vpn_derived",
        project_id="moz-fx-data-shared-prod",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    with TaskGroup(
        "mozilla_vpn_derived__all_subscriptions__v1_external",
    ) as mozilla_vpn_derived__all_subscriptions__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_mozilla_vpn_site_metrics__wait_for_mozilla_vpn_derived__all_subscriptions__v1",
            external_dag_id="bqetl_mozilla_vpn_site_metrics",
            external_task_id="wait_for_mozilla_vpn_derived__all_subscriptions__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=38700)).isoformat() }}",
        )

        mozilla_vpn_derived__all_subscriptions__v1_external.set_upstream(
            mozilla_vpn_derived__all_subscriptions__v1
        )

    mozilla_vpn_derived__channel_group_proportions__v1 = bigquery_etl_query(
        task_id="mozilla_vpn_derived__channel_group_proportions__v1",
        destination_table='channel_group_proportions_v1${{ macros.ds_format(macros.ds_add(ds, -7), "%Y-%m-%d", "%Y%m%d") }}',
        dataset_id="mozilla_vpn_derived",
        project_id="moz-fx-data-shared-prod",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        parameters=["date:DATE:{{macros.ds_add(ds, -7)}}"],
    )

    mozilla_vpn_derived__devices__v1 = bigquery_etl_query(
        task_id="mozilla_vpn_derived__devices__v1",
        destination_table="devices_v1",
        dataset_id="mozilla_vpn_derived",
        project_id="moz-fx-data-shared-prod",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    mozilla_vpn_derived__exchange_rates__v1 = gke_command(
        task_id="mozilla_vpn_derived__exchange_rates__v1",
        command=[
            "python",
            "sql/moz-fx-data-shared-prod/mozilla_vpn_derived/exchange_rates_v1/query.py",
        ]
        + [
            "--start-date",
            "{{ ds }}",
            "--end-date",
            "{{ ds }}",
            "--table",
            "moz-fx-data-shared-prod.mozilla_vpn_derived.exchange_rates_v1",
            "--base-currencies",
            "EUR",
            "CHF",
            "GBP",
            "CAD",
            "SGD",
            "NZD",
            "MYR",
            "AED",
            "BRL",
            "CLP",
            "COP",
            "EGP",
            "IDR",
            "MMK",
            "MXN",
            "PHP",
            "QAR",
            "RUB",
            "SAR",
            "SEK",
            "THB",
            "TZS",
            "UAH",
            "BGN",
            "CZK",
            "DKK",
            "HUF",
            "PLN",
            "RON",
        ],
        docker_image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        gcp_conn_id="google_cloud_airflow_gke",
        gke_project_id="moz-fx-data-airflow-gke-prod",
        gke_location="us-west1",
        gke_cluster_name="workloads-prod-v1",
        retry_delay=datetime.timedelta(seconds=300),
    )

    mozilla_vpn_derived__funnel_fxa_login_to_protected__v1 = bigquery_etl_query(
        task_id="mozilla_vpn_derived__funnel_fxa_login_to_protected__v1",
        destination_table="funnel_fxa_login_to_protected_v1",
        dataset_id="mozilla_vpn_derived",
        project_id="moz-fx-data-shared-prod",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    mozilla_vpn_derived__funnel_product_page_to_subscribed__v1 = bigquery_etl_query(
        task_id="mozilla_vpn_derived__funnel_product_page_to_subscribed__v1",
        destination_table="funnel_product_page_to_subscribed_v1",
        dataset_id="mozilla_vpn_derived",
        project_id="moz-fx-data-shared-prod",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="date",
        depends_on_past=False,
    )

    mozilla_vpn_derived__fxa_attribution__v1 = bigquery_etl_query(
        task_id="mozilla_vpn_derived__fxa_attribution__v1",
        destination_table="fxa_attribution_v1",
        dataset_id="mozilla_vpn_derived",
        project_id="moz-fx-data-shared-prod",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
        parameters=["date:DATE:{{ds}}"],
    )

    mozilla_vpn_derived__guardian_apple_events__v1 = bigquery_etl_query(
        task_id="mozilla_vpn_derived__guardian_apple_events__v1",
        destination_table="guardian_apple_events_v1",
        dataset_id="mozilla_vpn_derived",
        project_id="moz-fx-data-shared-prod",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    mozilla_vpn_derived__login_flows__v1 = bigquery_etl_query(
        task_id="mozilla_vpn_derived__login_flows__v1",
        destination_table="login_flows_v1",
        dataset_id="mozilla_vpn_derived",
        project_id="moz-fx-data-shared-prod",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
        parameters=["date:DATE:{{ds}}"],
    )

    mozilla_vpn_derived__protected__v1 = bigquery_etl_query(
        task_id="mozilla_vpn_derived__protected__v1",
        destination_table="protected_v1",
        dataset_id="mozilla_vpn_derived",
        project_id="moz-fx-data-shared-prod",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
        parameters=["date:DATE:{{ds}}"],
    )

    mozilla_vpn_derived__subscription_events__v1 = bigquery_etl_query(
        task_id="mozilla_vpn_derived__subscription_events__v1",
        destination_table='subscription_events_v1${{ macros.ds_format(macros.ds_add(ds, -8), "%Y-%m-%d", "%Y%m%d") }}',
        dataset_id="mozilla_vpn_derived",
        project_id="moz-fx-data-shared-prod",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        parameters=["date:DATE:{{macros.ds_add(ds, -8)}}"],
    )

    mozilla_vpn_derived__subscriptions__v1 = bigquery_etl_query(
        task_id="mozilla_vpn_derived__subscriptions__v1",
        destination_table="subscriptions_v1",
        dataset_id="mozilla_vpn_derived",
        project_id="moz-fx-data-shared-prod",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    mozilla_vpn_derived__survey_cancellation_of_service__v1 = gke_command(
        task_id="mozilla_vpn_derived__survey_cancellation_of_service__v1",
        command=[
            "python",
            "sql/moz-fx-data-shared-prod/mozilla_vpn_derived/survey_cancellation_of_service_v1/query.py",
        ]
        + [
            "--date",
            "{{ ds }}",
            "--survey_id",
            "5111573",
            "--api_token",
            "{{ var.value.surveygizmo_api_token }}",
            "--api_secret",
            "{{ var.value.surveygizmo_api_secret }}",
            "--destination_table",
            "moz-fx-data-shared-prod.mozilla_vpn_derived.survey_cancellation_of_service_v1",
        ],
        docker_image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
        owner="amiyaguchi@mozilla.com",
        email=[
            "amiyaguchi@mozilla.com",
            "srose@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
    )

    mozilla_vpn_derived__survey_intercept_q3__v1 = gke_command(
        task_id="mozilla_vpn_derived__survey_intercept_q3__v1",
        command=[
            "python",
            "sql/moz-fx-data-shared-prod/mozilla_vpn_derived/survey_intercept_q3_v1/query.py",
        ]
        + [
            "--date",
            "{{ ds }}",
            "--survey_id",
            "5829956",
            "--api_token",
            "{{ var.value.surveygizmo_api_token }}",
            "--api_secret",
            "{{ var.value.surveygizmo_api_secret }}",
            "--destination_table",
            "moz-fx-data-shared-prod.mozilla_vpn_derived.survey_intercept_q3_v1",
        ],
        docker_image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
    )

    mozilla_vpn_derived__survey_lifecycle_28d_desktop__v1 = gke_command(
        task_id="mozilla_vpn_derived__survey_lifecycle_28d_desktop__v1",
        command=[
            "python",
            "sql/moz-fx-data-shared-prod/mozilla_vpn_derived/survey_lifecycle_28d_desktop_v1/query.py",
        ]
        + [
            "--date",
            "{{ ds }}",
            "--survey_id",
            "6897437",
            "--api_token",
            "{{ var.value.surveygizmo_api_token }}",
            "--api_secret",
            "{{ var.value.surveygizmo_api_secret }}",
            "--destination_table",
            "moz-fx-data-shared-prod.mozilla_vpn_derived.survey_lifecycle_28d_desktop_v1",
        ],
        docker_image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
    )

    mozilla_vpn_derived__survey_lifecycle_28d_mobile__v1 = gke_command(
        task_id="mozilla_vpn_derived__survey_lifecycle_28d_mobile__v1",
        command=[
            "python",
            "sql/moz-fx-data-shared-prod/mozilla_vpn_derived/survey_lifecycle_28d_mobile_v1/query.py",
        ]
        + [
            "--date",
            "{{ ds }}",
            "--survey_id",
            "6897488",
            "--api_token",
            "{{ var.value.surveygizmo_api_token }}",
            "--api_secret",
            "{{ var.value.surveygizmo_api_secret }}",
            "--destination_table",
            "moz-fx-data-shared-prod.mozilla_vpn_derived.survey_lifecycle_28d_mobile_v1",
        ],
        docker_image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
    )

    mozilla_vpn_derived__survey_market_fit__v1 = gke_command(
        task_id="mozilla_vpn_derived__survey_market_fit__v1",
        command=[
            "python",
            "sql/moz-fx-data-shared-prod/mozilla_vpn_derived/survey_market_fit_v1/query.py",
        ]
        + [
            "--date",
            "{{ ds }}",
            "--survey_id",
            "5205593",
            "--api_token",
            "{{ var.value.surveygizmo_api_token }}",
            "--api_secret",
            "{{ var.value.surveygizmo_api_secret }}",
            "--destination_table",
            "moz-fx-data-shared-prod.mozilla_vpn_derived.survey_market_fit_v1",
        ],
        docker_image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
    )

    mozilla_vpn_derived__survey_product_quality__v1 = gke_command(
        task_id="mozilla_vpn_derived__survey_product_quality__v1",
        command=[
            "python",
            "sql/moz-fx-data-shared-prod/mozilla_vpn_derived/survey_product_quality_v1/query.py",
        ]
        + [
            "--date",
            "{{ ds }}",
            "--survey_id",
            "5187896",
            "--api_token",
            "{{ var.value.surveygizmo_api_token }}",
            "--api_secret",
            "{{ var.value.surveygizmo_api_secret }}",
            "--destination_table",
            "moz-fx-data-shared-prod.mozilla_vpn_derived.survey_product_quality_v1",
        ],
        docker_image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
    )

    mozilla_vpn_derived__survey_recommend__v1 = gke_command(
        task_id="mozilla_vpn_derived__survey_recommend__v1",
        command=[
            "python",
            "sql/moz-fx-data-shared-prod/mozilla_vpn_derived/survey_recommend_v1/query.py",
        ]
        + [
            "--date",
            "{{ ds }}",
            "--survey_id",
            "5572350",
            "--api_token",
            "{{ var.value.surveygizmo_api_token }}",
            "--api_secret",
            "{{ var.value.surveygizmo_api_secret }}",
            "--destination_table",
            "moz-fx-data-shared-prod.mozilla_vpn_derived.survey_recommend_v1",
        ],
        docker_image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
    )

    mozilla_vpn_derived__users__v1 = bigquery_etl_query(
        task_id="mozilla_vpn_derived__users__v1",
        destination_table="users_v1",
        dataset_id="mozilla_vpn_derived",
        project_id="moz-fx-data-shared-prod",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    mozilla_vpn_derived__vat_rates__v1 = gke_command(
        task_id="mozilla_vpn_derived__vat_rates__v1",
        command=[
            "python",
            "sql/moz-fx-data-shared-prod/mozilla_vpn_derived/vat_rates_v1/query.py",
        ]
        + [],
        docker_image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        gcp_conn_id="google_cloud_airflow_gke",
        gke_project_id="moz-fx-data-airflow-gke-prod",
        gke_location="us-west1",
        gke_cluster_name="workloads-prod-v1",
        retry_delay=datetime.timedelta(seconds=300),
    )

    mozilla_vpn_external__devices__v1 = bigquery_etl_query(
        task_id="mozilla_vpn_external__devices__v1",
        destination_table="devices_v1",
        dataset_id="mozilla_vpn_external",
        project_id="moz-fx-data-shared-prod",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=True,
        parameters=[
            "external_database_query:STRING: SELECT\n  id,\n  user_id,\n  name,\n  mullvad_id,\n  pubkey,\n  ipv4_address,\n  ipv6_address,\n  created_at,\n  updated_at,\n  uid,\n  platform,\n  useragent,\n  unique_id\nFROM devices WHERE DATE(updated_at) = DATE '{{ds}}'"
        ],
    )

    mozilla_vpn_external__subscriptions__v1 = bigquery_etl_query(
        task_id="mozilla_vpn_external__subscriptions__v1",
        destination_table="subscriptions_v1",
        dataset_id="mozilla_vpn_external",
        project_id="moz-fx-data-shared-prod",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=True,
        parameters=[
            "external_database_query:STRING: SELECT\n  id,\n  user_id,\n  is_active,\n  mullvad_token,\n  mullvad_account_created_at,\n  mullvad_account_expiration_date,\n  ended_at,\n  created_at,\n  updated_at,\n  type,\n  fxa_last_changed_at,\n  fxa_migration_note\nFROM subscriptions WHERE DATE(updated_at) = DATE '{{ds}}'"
        ],
    )

    mozilla_vpn_external__users__v1 = bigquery_etl_query(
        task_id="mozilla_vpn_external__users__v1",
        destination_table="users_v1",
        dataset_id="mozilla_vpn_external",
        project_id="moz-fx-data-shared-prod",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=True,
        parameters=[
            "external_database_query:STRING: SELECT\n  id,\n  email,\n  fxa_uid,\n  fxa_profile_json,\n  created_at,\n  updated_at,\n  display_name,\n  avatar\nFROM users WHERE DATE(updated_at) = DATE '{{ds}}'"
        ],
    )

    relay_derived__active_subscription_ids__v1 = bigquery_etl_query(
        task_id="relay_derived__active_subscription_ids__v1",
        destination_table='active_subscription_ids_v1${{ macros.ds_format(macros.ds_add(ds, -7), "%Y-%m-%d", "%Y%m%d") }}',
        dataset_id="relay_derived",
        project_id="moz-fx-data-shared-prod",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=True,
        parameters=["date:DATE:{{macros.ds_add(ds, -7)}}"],
    )

    relay_derived__active_subscriptions__v1 = bigquery_etl_query(
        task_id="relay_derived__active_subscriptions__v1",
        destination_table='active_subscriptions_v1${{ macros.ds_format(macros.ds_add(ds, -7), "%Y-%m-%d", "%Y%m%d") }}',
        dataset_id="relay_derived",
        project_id="moz-fx-data-shared-prod",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        parameters=["date:DATE:{{macros.ds_add(ds, -7)}}"],
    )

    relay_derived__subscription_events__v1 = bigquery_etl_query(
        task_id="relay_derived__subscription_events__v1",
        destination_table='subscription_events_v1${{ macros.ds_format(macros.ds_add(ds, -8), "%Y-%m-%d", "%Y%m%d") }}',
        dataset_id="relay_derived",
        project_id="moz-fx-data-shared-prod",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        parameters=["date:DATE:{{macros.ds_add(ds, -8)}}"],
    )

    relay_derived__subscriptions__v1 = bigquery_etl_query(
        task_id="relay_derived__subscriptions__v1",
        destination_table="subscriptions_v1",
        dataset_id="relay_derived",
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

    stripe_external__itemized_payout_reconciliation__v5 = gke_command(
        task_id="stripe_external__itemized_payout_reconciliation__v5",
        command=[
            "python",
            "sql/moz-fx-data-shared-prod/stripe_external/itemized_payout_reconciliation_v5/query.py",
        ]
        + [
            "--date={{ ds }}",
            "--api-key={{ var.value.stripe_api_key }}",
            "--report-type=payout_reconciliation.itemized.5",
            "--table=moz-fx-data-shared-prod.stripe_external.itemized_payout_reconciliation_v5",
            "--time-partitioning-field=automatic_payout_effective_at",
        ],
        docker_image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        retry_delay=datetime.timedelta(seconds=1800),
        retries=47,
        email_on_retry=False,
    )

    stripe_external__itemized_tax_transactions__v1 = gke_command(
        task_id="stripe_external__itemized_tax_transactions__v1",
        command=[
            "python",
            "sql/moz-fx-data-shared-prod/stripe_external/itemized_tax_transactions_v1/query.py",
        ]
        + [
            "--date={{ ds }}",
            "--api-key={{ var.value.stripe_api_key }}",
            "--report-type=tax.transactions.itemized.1",
            "--table=moz-fx-data-shared-prod.stripe_external.itemized_tax_transactions_v1",
            "--time-partitioning-field=transaction_date_utc",
        ],
        docker_image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        retry_delay=datetime.timedelta(seconds=1800),
        retries=47,
        email_on_retry=False,
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
        date_partition_parameter="date",
        depends_on_past=False,
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

    subscription_platform_derived__active_subscriptions__v1 = bigquery_etl_query(
        task_id="subscription_platform_derived__active_subscriptions__v1",
        destination_table="active_subscriptions_v1",
        dataset_id="subscription_platform_derived",
        project_id="moz-fx-data-shared-prod",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    subscription_platform_derived__apple_subscriptions__v1 = bigquery_etl_query(
        task_id="subscription_platform_derived__apple_subscriptions__v1",
        destination_table="apple_subscriptions_v1",
        dataset_id="subscription_platform_derived",
        project_id="moz-fx-data-shared-prod",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    subscription_platform_derived__daily_active_logical_subscriptions__v1 = bigquery_etl_query(
        task_id="subscription_platform_derived__daily_active_logical_subscriptions__v1",
        destination_table="daily_active_logical_subscriptions_v1",
        dataset_id="subscription_platform_derived",
        project_id="moz-fx-data-shared-prod",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="date",
        depends_on_past=False,
    )

    subscription_platform_derived__google_subscriptions__v1 = bigquery_etl_query(
        task_id="subscription_platform_derived__google_subscriptions__v1",
        destination_table="google_subscriptions_v1",
        dataset_id="subscription_platform_derived",
        project_id="moz-fx-data-shared-prod",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    subscription_platform_derived__logical_subscription_events__v1 = bigquery_etl_query(
        task_id="subscription_platform_derived__logical_subscription_events__v1",
        destination_table="logical_subscription_events_v1",
        dataset_id="subscription_platform_derived",
        project_id="moz-fx-data-shared-prod",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="date",
        depends_on_past=False,
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

    subscription_platform_derived__monthly_active_logical_subscriptions__v1 = bigquery_etl_query(
        task_id="subscription_platform_derived__monthly_active_logical_subscriptions__v1",
        destination_table="monthly_active_logical_subscriptions_v1",
        dataset_id="subscription_platform_derived",
        project_id="moz-fx-data-shared-prod",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="date",
        table_partition_template='${{ dag_run.logical_date.strftime("%Y%m") }}',
        depends_on_past=False,
    )

    subscription_platform_derived__nonprod_apple_subscriptions__v1 = bigquery_etl_query(
        task_id="subscription_platform_derived__nonprod_apple_subscriptions__v1",
        destination_table="nonprod_apple_subscriptions_v1",
        dataset_id="subscription_platform_derived",
        project_id="moz-fx-data-shared-prod",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    subscription_platform_derived__nonprod_google_subscriptions__v1 = (
        bigquery_etl_query(
            task_id="subscription_platform_derived__nonprod_google_subscriptions__v1",
            destination_table="nonprod_google_subscriptions_v1",
            dataset_id="subscription_platform_derived",
            project_id="moz-fx-data-shared-prod",
            owner="srose@mozilla.com",
            email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
            date_partition_parameter=None,
            depends_on_past=False,
            task_concurrency=1,
        )
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

    subscription_platform_derived__stripe_subscriptions__v1 = bigquery_etl_query(
        task_id="subscription_platform_derived__stripe_subscriptions__v1",
        destination_table="stripe_subscriptions_v1",
        dataset_id="subscription_platform_derived",
        project_id="moz-fx-data-shared-prod",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
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

    subscription_platform_derived__stripe_subscriptions_history__v1 = (
        bigquery_etl_query(
            task_id="subscription_platform_derived__stripe_subscriptions_history__v1",
            destination_table="stripe_subscriptions_history_v1",
            dataset_id="subscription_platform_derived",
            project_id="moz-fx-data-shared-prod",
            owner="srose@mozilla.com",
            email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
            date_partition_parameter=None,
            depends_on_past=False,
            task_concurrency=1,
        )
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

    subscription_platform_derived__subplat_attribution_impressions__v1 = bigquery_etl_query(
        task_id="subscription_platform_derived__subplat_attribution_impressions__v1",
        destination_table="subplat_attribution_impressions_v1",
        dataset_id="subscription_platform_derived",
        project_id="moz-fx-data-shared-prod",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=True,
        parameters=["date:DATE:{{ds}}"],
    )

    subscription_platform_derived__subplat_flow_events__v1 = bigquery_etl_query(
        task_id="subscription_platform_derived__subplat_flow_events__v1",
        destination_table="subplat_flow_events_v1",
        dataset_id="subscription_platform_derived",
        project_id="moz-fx-data-shared-prod",
        owner="srose@mozilla.com",
        email=["srose@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="date",
        depends_on_past=True,
    )

    wait_for_firefox_accounts_derived__fxa_auth_events__v1 = ExternalTaskSensor(
        task_id="wait_for_firefox_accounts_derived__fxa_auth_events__v1",
        external_dag_id="bqetl_fxa_events",
        external_task_id="firefox_accounts_derived__fxa_auth_events__v1",
        execution_delta=datetime.timedelta(seconds=900),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    cjms_bigquery__flows__v1.set_upstream(
        wait_for_firefox_accounts_derived__fxa_auth_events__v1
    )
    wait_for_firefox_accounts_derived__fxa_gcp_stderr_events__v1 = ExternalTaskSensor(
        task_id="wait_for_firefox_accounts_derived__fxa_gcp_stderr_events__v1",
        external_dag_id="bqetl_fxa_events",
        external_task_id="firefox_accounts_derived__fxa_gcp_stderr_events__v1",
        execution_delta=datetime.timedelta(seconds=900),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    cjms_bigquery__flows__v1.set_upstream(
        wait_for_firefox_accounts_derived__fxa_gcp_stderr_events__v1
    )
    wait_for_firefox_accounts_derived__fxa_gcp_stdout_events__v1 = ExternalTaskSensor(
        task_id="wait_for_firefox_accounts_derived__fxa_gcp_stdout_events__v1",
        external_dag_id="bqetl_fxa_events",
        external_task_id="firefox_accounts_derived__fxa_gcp_stdout_events__v1",
        execution_delta=datetime.timedelta(seconds=900),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    cjms_bigquery__flows__v1.set_upstream(
        wait_for_firefox_accounts_derived__fxa_gcp_stdout_events__v1
    )
    wait_for_firefox_accounts_derived__fxa_stdout_events__v1 = ExternalTaskSensor(
        task_id="wait_for_firefox_accounts_derived__fxa_stdout_events__v1",
        external_dag_id="bqetl_fxa_events",
        external_task_id="firefox_accounts_derived__fxa_stdout_events__v1",
        execution_delta=datetime.timedelta(seconds=900),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    cjms_bigquery__flows__v1.set_upstream(
        wait_for_firefox_accounts_derived__fxa_stdout_events__v1
    )

    cjms_bigquery__refunds__v1.set_upstream(stripe_external__charge__v1)

    cjms_bigquery__refunds__v1.set_upstream(stripe_external__invoice__v1)

    cjms_bigquery__refunds__v1.set_upstream(stripe_external__refund__v1)

    cjms_bigquery__subscriptions__v1.set_upstream(cjms_bigquery__flows__v1)

    cjms_bigquery__subscriptions__v1.set_upstream(stripe_external__coupon__v1)

    cjms_bigquery__subscriptions__v1.set_upstream(stripe_external__invoice__v1)

    cjms_bigquery__subscriptions__v1.set_upstream(stripe_external__invoice_discount__v1)

    cjms_bigquery__subscriptions__v1.set_upstream(stripe_external__promotion_code__v1)

    cjms_bigquery__subscriptions__v1.set_upstream(
        subscription_platform_derived__stripe_subscriptions__v1
    )

    hubs_derived__active_subscription_ids__v1.set_upstream(
        hubs_derived__subscriptions__v1
    )

    hubs_derived__active_subscriptions__v1.set_upstream(
        hubs_derived__active_subscription_ids__v1
    )

    hubs_derived__active_subscriptions__v1.set_upstream(hubs_derived__subscriptions__v1)

    hubs_derived__subscription_events__v1.set_upstream(
        hubs_derived__active_subscription_ids__v1
    )

    hubs_derived__subscription_events__v1.set_upstream(hubs_derived__subscriptions__v1)

    hubs_derived__subscriptions__v1.set_upstream(
        subscription_platform_derived__stripe_subscriptions_history__v1
    )

    mozilla_vpn_derived__active_subscription_ids__v1.set_upstream(
        mozilla_vpn_derived__all_subscriptions__v1
    )

    mozilla_vpn_derived__active_subscriptions__v1.set_upstream(
        mozilla_vpn_derived__active_subscription_ids__v1
    )

    mozilla_vpn_derived__active_subscriptions__v1.set_upstream(
        mozilla_vpn_derived__all_subscriptions__v1
    )

    mozilla_vpn_derived__all_subscriptions__v1.set_upstream(
        mozilla_vpn_derived__fxa_attribution__v1
    )

    mozilla_vpn_derived__all_subscriptions__v1.set_upstream(
        mozilla_vpn_derived__users__v1
    )

    mozilla_vpn_derived__all_subscriptions__v1.set_upstream(
        subscription_platform_derived__apple_subscriptions__v1
    )

    mozilla_vpn_derived__all_subscriptions__v1.set_upstream(
        subscription_platform_derived__google_subscriptions__v1
    )

    mozilla_vpn_derived__all_subscriptions__v1.set_upstream(
        subscription_platform_derived__stripe_subscriptions_history__v1
    )

    mozilla_vpn_derived__channel_group_proportions__v1.set_upstream(
        mozilla_vpn_derived__active_subscription_ids__v1
    )

    mozilla_vpn_derived__channel_group_proportions__v1.set_upstream(
        mozilla_vpn_derived__all_subscriptions__v1
    )

    mozilla_vpn_derived__channel_group_proportions__v1.set_upstream(
        mozilla_vpn_derived__subscription_events__v1
    )

    mozilla_vpn_derived__devices__v1.set_upstream(mozilla_vpn_external__devices__v1)

    mozilla_vpn_derived__funnel_fxa_login_to_protected__v1.set_upstream(
        mozilla_vpn_derived__add_device_events__v1
    )

    mozilla_vpn_derived__funnel_fxa_login_to_protected__v1.set_upstream(
        mozilla_vpn_derived__all_subscriptions__v1
    )

    mozilla_vpn_derived__funnel_fxa_login_to_protected__v1.set_upstream(
        mozilla_vpn_derived__login_flows__v1
    )

    mozilla_vpn_derived__funnel_fxa_login_to_protected__v1.set_upstream(
        mozilla_vpn_derived__protected__v1
    )

    mozilla_vpn_derived__funnel_fxa_login_to_protected__v1.set_upstream(
        mozilla_vpn_derived__users__v1
    )

    mozilla_vpn_derived__funnel_product_page_to_subscribed__v1.set_upstream(
        wait_for_firefox_accounts_derived__fxa_auth_events__v1
    )
    mozilla_vpn_derived__funnel_product_page_to_subscribed__v1.set_upstream(
        wait_for_firefox_accounts_derived__fxa_gcp_stderr_events__v1
    )
    mozilla_vpn_derived__funnel_product_page_to_subscribed__v1.set_upstream(
        wait_for_firefox_accounts_derived__fxa_gcp_stdout_events__v1
    )
    mozilla_vpn_derived__funnel_product_page_to_subscribed__v1.set_upstream(
        wait_for_firefox_accounts_derived__fxa_stdout_events__v1
    )

    mozilla_vpn_derived__funnel_product_page_to_subscribed__v1.set_upstream(
        stripe_external__plan__v1
    )

    mozilla_vpn_derived__funnel_product_page_to_subscribed__v1.set_upstream(
        stripe_external__product__v1
    )

    mozilla_vpn_derived__fxa_attribution__v1.set_upstream(
        wait_for_firefox_accounts_derived__fxa_auth_events__v1
    )
    mozilla_vpn_derived__fxa_attribution__v1.set_upstream(
        wait_for_firefox_accounts_derived__fxa_gcp_stderr_events__v1
    )
    mozilla_vpn_derived__fxa_attribution__v1.set_upstream(
        wait_for_firefox_accounts_derived__fxa_gcp_stdout_events__v1
    )
    mozilla_vpn_derived__fxa_attribution__v1.set_upstream(
        wait_for_firefox_accounts_derived__fxa_stdout_events__v1
    )

    mozilla_vpn_derived__guardian_apple_events__v1.set_upstream(
        mozilla_vpn_external__users__v1
    )

    mozilla_vpn_derived__login_flows__v1.set_upstream(
        wait_for_firefox_accounts_derived__fxa_auth_events__v1
    )
    mozilla_vpn_derived__login_flows__v1.set_upstream(
        wait_for_firefox_accounts_derived__fxa_gcp_stderr_events__v1
    )
    mozilla_vpn_derived__login_flows__v1.set_upstream(
        wait_for_firefox_accounts_derived__fxa_gcp_stdout_events__v1
    )
    mozilla_vpn_derived__login_flows__v1.set_upstream(
        wait_for_firefox_accounts_derived__fxa_stdout_events__v1
    )

    mozilla_vpn_derived__subscription_events__v1.set_upstream(
        mozilla_vpn_derived__active_subscription_ids__v1
    )

    mozilla_vpn_derived__subscription_events__v1.set_upstream(
        mozilla_vpn_derived__all_subscriptions__v1
    )

    mozilla_vpn_derived__subscriptions__v1.set_upstream(
        mozilla_vpn_external__subscriptions__v1
    )

    mozilla_vpn_derived__users__v1.set_upstream(mozilla_vpn_external__users__v1)

    relay_derived__active_subscription_ids__v1.set_upstream(
        relay_derived__subscriptions__v1
    )

    relay_derived__active_subscriptions__v1.set_upstream(
        relay_derived__active_subscription_ids__v1
    )

    relay_derived__active_subscriptions__v1.set_upstream(
        relay_derived__subscriptions__v1
    )

    relay_derived__subscription_events__v1.set_upstream(
        relay_derived__active_subscription_ids__v1
    )

    relay_derived__subscription_events__v1.set_upstream(
        relay_derived__subscriptions__v1
    )

    relay_derived__subscriptions__v1.set_upstream(
        subscription_platform_derived__stripe_subscriptions_history__v1
    )

    fivetran_stripe_sync_start = FivetranOperator(
        connector_id="{{ var.value.fivetran_stripe_connector_id }}",
        task_id="fivetran_stripe_task",
    )

    fivetran_stripe_sync_wait = FivetranSensor(
        connector_id="{{ var.value.fivetran_stripe_connector_id }}",
        task_id="fivetran_stripe_sensor",
        poke_interval=30,
        xcom="{{ task_instance.xcom_pull('fivetran_stripe_task') }}",
        on_retry_callback=retry_tasks_callback,
        params={"retry_tasks": ["fivetran_stripe_task"]},
    )

    fivetran_stripe_sync_wait.set_upstream(fivetran_stripe_sync_start)

    stripe_external__card__v1.set_upstream(fivetran_stripe_sync_wait)

    stripe_external__charge__v1.set_upstream(fivetran_stripe_sync_wait)

    stripe_external__coupon__v1.set_upstream(fivetran_stripe_sync_wait)

    stripe_external__customer__v1.set_upstream(fivetran_stripe_sync_wait)

    stripe_external__customer_discount__v1.set_upstream(fivetran_stripe_sync_wait)

    stripe_external__customers_changelog__v1.set_upstream(stripe_external__coupon__v1)

    stripe_external__customers_changelog__v1.set_upstream(stripe_external__customer__v1)

    stripe_external__customers_changelog__v1.set_upstream(
        stripe_external__customer_discount__v1
    )

    stripe_external__invoice__v1.set_upstream(fivetran_stripe_sync_wait)

    stripe_external__invoice_discount__v1.set_upstream(fivetran_stripe_sync_wait)

    stripe_external__invoice_line_item__v1.set_upstream(fivetran_stripe_sync_wait)

    stripe_external__plan__v1.set_upstream(fivetran_stripe_sync_wait)

    stripe_external__product__v1.set_upstream(fivetran_stripe_sync_wait)

    stripe_external__promotion_code__v1.set_upstream(fivetran_stripe_sync_wait)

    stripe_external__refund__v1.set_upstream(fivetran_stripe_sync_wait)

    stripe_external__subscription_discount__v1.set_upstream(fivetran_stripe_sync_wait)

    stripe_external__subscription_history__v1.set_upstream(fivetran_stripe_sync_wait)

    stripe_external__subscription_item__v1.set_upstream(fivetran_stripe_sync_wait)

    stripe_external__subscription_tax_rate__v1.set_upstream(fivetran_stripe_sync_wait)

    stripe_external__subscriptions_changelog__v1.set_upstream(
        stripe_external__coupon__v1
    )

    stripe_external__subscriptions_changelog__v1.set_upstream(stripe_external__plan__v1)

    stripe_external__subscriptions_changelog__v1.set_upstream(
        stripe_external__product__v1
    )

    stripe_external__subscriptions_changelog__v1.set_upstream(
        stripe_external__subscription_discount__v1
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

    stripe_external__tax_rate__v1.set_upstream(fivetran_stripe_sync_wait)

    subscription_platform_derived__active_subscriptions__v1.set_upstream(
        hubs_derived__active_subscription_ids__v1
    )

    subscription_platform_derived__active_subscriptions__v1.set_upstream(
        hubs_derived__active_subscriptions__v1
    )

    subscription_platform_derived__active_subscriptions__v1.set_upstream(
        hubs_derived__subscriptions__v1
    )

    subscription_platform_derived__active_subscriptions__v1.set_upstream(
        mozilla_vpn_derived__active_subscription_ids__v1
    )

    subscription_platform_derived__active_subscriptions__v1.set_upstream(
        mozilla_vpn_derived__active_subscriptions__v1
    )

    subscription_platform_derived__active_subscriptions__v1.set_upstream(
        mozilla_vpn_derived__all_subscriptions__v1
    )

    subscription_platform_derived__active_subscriptions__v1.set_upstream(
        relay_derived__active_subscription_ids__v1
    )

    subscription_platform_derived__active_subscriptions__v1.set_upstream(
        relay_derived__active_subscriptions__v1
    )

    subscription_platform_derived__active_subscriptions__v1.set_upstream(
        relay_derived__subscriptions__v1
    )

    subscription_platform_derived__apple_subscriptions__v1.set_upstream(
        mozilla_vpn_derived__guardian_apple_events__v1
    )

    subscription_platform_derived__daily_active_logical_subscriptions__v1.set_upstream(
        subscription_platform_derived__logical_subscriptions_history__v1
    )

    subscription_platform_derived__logical_subscription_events__v1.set_upstream(
        subscription_platform_derived__logical_subscriptions_history__v1
    )

    subscription_platform_derived__logical_subscriptions_history__v1.set_upstream(
        mozilla_vpn_derived__users__v1
    )

    subscription_platform_derived__logical_subscriptions_history__v1.set_upstream(
        subscription_platform_derived__stripe_logical_subscriptions_history__v1
    )

    subscription_platform_derived__logical_subscriptions_history__v1.set_upstream(
        subscription_platform_derived__subplat_attribution_impressions__v1
    )

    subscription_platform_derived__monthly_active_logical_subscriptions__v1.set_upstream(
        subscription_platform_derived__daily_active_logical_subscriptions__v1
    )

    subscription_platform_derived__nonprod_apple_subscriptions__v1.set_upstream(
        mozilla_vpn_derived__guardian_apple_events__v1
    )

    subscription_platform_derived__services__v1.set_upstream(
        subscription_platform_derived__stripe_plans__v1
    )

    subscription_platform_derived__services__v1.set_upstream(
        subscription_platform_derived__stripe_products__v1
    )

    subscription_platform_derived__stripe_customers_history__v1.set_upstream(
        subscription_platform_derived__stripe_customers_revised_changelog__v1
    )

    subscription_platform_derived__stripe_customers_revised_changelog__v1.set_upstream(
        stripe_external__customers_changelog__v1
    )

    subscription_platform_derived__stripe_customers_revised_changelog__v1.set_upstream(
        stripe_external__subscriptions_changelog__v1
    )

    subscription_platform_derived__stripe_logical_subscriptions_history__v1.set_upstream(
        stripe_external__card__v1
    )

    subscription_platform_derived__stripe_logical_subscriptions_history__v1.set_upstream(
        stripe_external__charge__v1
    )

    subscription_platform_derived__stripe_logical_subscriptions_history__v1.set_upstream(
        stripe_external__invoice__v1
    )

    subscription_platform_derived__stripe_logical_subscriptions_history__v1.set_upstream(
        stripe_external__refund__v1
    )

    subscription_platform_derived__stripe_logical_subscriptions_history__v1.set_upstream(
        subscription_platform_derived__services__v1
    )

    subscription_platform_derived__stripe_logical_subscriptions_history__v1.set_upstream(
        subscription_platform_derived__stripe_subscriptions_history__v2
    )

    subscription_platform_derived__stripe_plans__v1.set_upstream(
        stripe_external__plan__v1
    )

    subscription_platform_derived__stripe_products__v1.set_upstream(
        stripe_external__product__v1
    )

    subscription_platform_derived__stripe_subscriptions__v1.set_upstream(
        subscription_platform_derived__stripe_subscriptions_history__v1
    )

    subscription_platform_derived__stripe_subscriptions__v2.set_upstream(
        subscription_platform_derived__stripe_subscriptions_history__v2
    )

    subscription_platform_derived__stripe_subscriptions_history__v1.set_upstream(
        stripe_external__card__v1
    )

    subscription_platform_derived__stripe_subscriptions_history__v1.set_upstream(
        stripe_external__charge__v1
    )

    subscription_platform_derived__stripe_subscriptions_history__v1.set_upstream(
        stripe_external__coupon__v1
    )

    subscription_platform_derived__stripe_subscriptions_history__v1.set_upstream(
        stripe_external__customer__v1
    )

    subscription_platform_derived__stripe_subscriptions_history__v1.set_upstream(
        stripe_external__invoice__v1
    )

    subscription_platform_derived__stripe_subscriptions_history__v1.set_upstream(
        stripe_external__invoice_discount__v1
    )

    subscription_platform_derived__stripe_subscriptions_history__v1.set_upstream(
        stripe_external__plan__v1
    )

    subscription_platform_derived__stripe_subscriptions_history__v1.set_upstream(
        stripe_external__product__v1
    )

    subscription_platform_derived__stripe_subscriptions_history__v1.set_upstream(
        stripe_external__promotion_code__v1
    )

    subscription_platform_derived__stripe_subscriptions_history__v1.set_upstream(
        stripe_external__refund__v1
    )

    subscription_platform_derived__stripe_subscriptions_history__v1.set_upstream(
        stripe_external__subscription_history__v1
    )

    subscription_platform_derived__stripe_subscriptions_history__v1.set_upstream(
        stripe_external__subscription_item__v1
    )

    subscription_platform_derived__stripe_subscriptions_history__v2.set_upstream(
        subscription_platform_derived__stripe_customers_history__v1
    )

    subscription_platform_derived__stripe_subscriptions_history__v2.set_upstream(
        subscription_platform_derived__stripe_subscriptions_revised_changelog__v1
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

    subscription_platform_derived__subplat_attribution_impressions__v1.set_upstream(
        subscription_platform_derived__services__v1
    )

    subscription_platform_derived__subplat_attribution_impressions__v1.set_upstream(
        subscription_platform_derived__subplat_flow_events__v1
    )

    subscription_platform_derived__subplat_flow_events__v1.set_upstream(
        wait_for_firefox_accounts_derived__fxa_auth_events__v1
    )
    subscription_platform_derived__subplat_flow_events__v1.set_upstream(
        wait_for_firefox_accounts_derived__fxa_gcp_stderr_events__v1
    )
    subscription_platform_derived__subplat_flow_events__v1.set_upstream(
        wait_for_firefox_accounts_derived__fxa_gcp_stdout_events__v1
    )
    subscription_platform_derived__subplat_flow_events__v1.set_upstream(
        wait_for_firefox_accounts_derived__fxa_stdout_events__v1
    )

    subscription_platform_derived__subplat_flow_events__v1.set_upstream(
        subscription_platform_derived__services__v1
    )
