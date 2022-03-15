# Generated via https://github.com/mozilla/bigquery-etl/blob/main/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from operators.task_sensor import ExternalTaskCompletedSensor
import datetime
from utils.gcp import bigquery_etl_query, gke_command

docs = """
### bqetl_subplat

Built from bigquery-etl repo, [`dags/bqetl_subplat.py`](https://github.com/mozilla/bigquery-etl/blob/main/dags/bqetl_subplat.py)

#### Description

Daily imports for Subscription Platform data from Stripe and the Mozilla VPN
operational DB as well as derived tables based on that data.

Depends on `bqetl_fxa_events`, so is scheduled to run a bit after that.

#### Owner

dthorn@mozilla.com
"""


default_args = {
    "owner": "dthorn@mozilla.com",
    "start_date": datetime.datetime(2021, 7, 20, 0, 0),
    "end_date": None,
    "email": ["telemetry-alerts@mozilla.com", "dthorn@mozilla.com"],
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

    mozilla_vpn_derived__active_subscriptions__v1 = bigquery_etl_query(
        task_id="mozilla_vpn_derived__active_subscriptions__v1",
        destination_table='active_subscriptions_v1${{ macros.ds_format(macros.ds_add(ds, -7), "%Y-%m-%d", "%Y%m%d") }}',
        dataset_id="mozilla_vpn_derived",
        project_id="moz-fx-data-shared-prod",
        owner="dthorn@mozilla.com",
        email=["dthorn@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        parameters=["date:DATE:{{macros.ds_add(ds, -7)}}"],
        sql_file_path="sql/moz-fx-data-shared-prod/mozilla_vpn_derived/active_subscriptions_v1/query.sql",
        dag=dag,
    )

    mozilla_vpn_derived__add_device_events__v1 = bigquery_etl_query(
        task_id="mozilla_vpn_derived__add_device_events__v1",
        destination_table="add_device_events_v1",
        dataset_id="mozilla_vpn_derived",
        project_id="moz-fx-data-shared-prod",
        owner="dthorn@mozilla.com",
        email=["dthorn@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        dag=dag,
    )

    mozilla_vpn_derived__all_subscriptions__v1 = bigquery_etl_query(
        task_id="mozilla_vpn_derived__all_subscriptions__v1",
        destination_table="all_subscriptions_v1",
        dataset_id="mozilla_vpn_derived",
        project_id="moz-fx-data-shared-prod",
        owner="dthorn@mozilla.com",
        email=["dthorn@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        dag=dag,
    )

    mozilla_vpn_derived__channel_group_proportions__v1 = bigquery_etl_query(
        task_id="mozilla_vpn_derived__channel_group_proportions__v1",
        destination_table='channel_group_proportions_v1${{ macros.ds_format(macros.ds_add(ds, -7), "%Y-%m-%d", "%Y%m%d") }}',
        dataset_id="mozilla_vpn_derived",
        project_id="moz-fx-data-shared-prod",
        owner="dthorn@mozilla.com",
        email=["dthorn@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        parameters=["date:DATE:{{macros.ds_add(ds, -7)}}"],
        sql_file_path="sql/moz-fx-data-shared-prod/mozilla_vpn_derived/channel_group_proportions_v1/query.sql",
        dag=dag,
    )

    mozilla_vpn_derived__devices__v1 = bigquery_etl_query(
        task_id="mozilla_vpn_derived__devices__v1",
        destination_table="devices_v1",
        dataset_id="mozilla_vpn_derived",
        project_id="moz-fx-data-shared-prod",
        owner="dthorn@mozilla.com",
        email=["dthorn@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        dag=dag,
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
        ],
        docker_image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
        owner="dthorn@mozilla.com",
        email=["dthorn@mozilla.com", "telemetry-alerts@mozilla.com"],
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
        owner="dthorn@mozilla.com",
        email=["dthorn@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        dag=dag,
    )

    mozilla_vpn_derived__funnel_product_page_to_subscribed__v1 = bigquery_etl_query(
        task_id="mozilla_vpn_derived__funnel_product_page_to_subscribed__v1",
        destination_table="funnel_product_page_to_subscribed_v1",
        dataset_id="mozilla_vpn_derived",
        project_id="moz-fx-data-shared-prod",
        owner="dthorn@mozilla.com",
        email=["dthorn@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter="date",
        depends_on_past=False,
        dag=dag,
    )

    mozilla_vpn_derived__fxa_attribution__v1 = bigquery_etl_query(
        task_id="mozilla_vpn_derived__fxa_attribution__v1",
        destination_table="fxa_attribution_v1",
        dataset_id="mozilla_vpn_derived",
        project_id="moz-fx-data-shared-prod",
        owner="dthorn@mozilla.com",
        email=["dthorn@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        parameters=["date:DATE:{{ds}}"],
        dag=dag,
    )

    mozilla_vpn_derived__login_flows__v1 = bigquery_etl_query(
        task_id="mozilla_vpn_derived__login_flows__v1",
        destination_table="login_flows_v1",
        dataset_id="mozilla_vpn_derived",
        project_id="moz-fx-data-shared-prod",
        owner="dthorn@mozilla.com",
        email=["dthorn@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        parameters=["date:DATE:{{ds}}"],
        dag=dag,
    )

    mozilla_vpn_derived__protected__v1 = bigquery_etl_query(
        task_id="mozilla_vpn_derived__protected__v1",
        destination_table="protected_v1",
        dataset_id="mozilla_vpn_derived",
        project_id="moz-fx-data-shared-prod",
        owner="dthorn@mozilla.com",
        email=["dthorn@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        parameters=["date:DATE:{{ds}}"],
        dag=dag,
    )

    mozilla_vpn_derived__subscription_events__v1 = bigquery_etl_query(
        task_id="mozilla_vpn_derived__subscription_events__v1",
        destination_table='subscription_events_v1${{ macros.ds_format(macros.ds_add(ds, -7), "%Y-%m-%d", "%Y%m%d") }}',
        dataset_id="mozilla_vpn_derived",
        project_id="moz-fx-data-shared-prod",
        owner="dthorn@mozilla.com",
        email=["dthorn@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        parameters=["date:DATE:{{macros.ds_add(ds, -7)}}"],
        sql_file_path="sql/moz-fx-data-shared-prod/mozilla_vpn_derived/subscription_events_v1/query.sql",
        dag=dag,
    )

    mozilla_vpn_derived__subscriptions__v1 = bigquery_etl_query(
        task_id="mozilla_vpn_derived__subscriptions__v1",
        destination_table="subscriptions_v1",
        dataset_id="mozilla_vpn_derived",
        project_id="moz-fx-data-shared-prod",
        owner="dthorn@mozilla.com",
        email=["dthorn@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        dag=dag,
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
            "dthorn@mozilla.com",
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
        owner="dthorn@mozilla.com",
        email=["dthorn@mozilla.com", "telemetry-alerts@mozilla.com"],
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
        owner="dthorn@mozilla.com",
        email=["dthorn@mozilla.com", "telemetry-alerts@mozilla.com"],
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
        owner="dthorn@mozilla.com",
        email=["dthorn@mozilla.com", "telemetry-alerts@mozilla.com"],
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
        owner="dthorn@mozilla.com",
        email=["dthorn@mozilla.com", "telemetry-alerts@mozilla.com"],
    )

    mozilla_vpn_derived__users__v1 = bigquery_etl_query(
        task_id="mozilla_vpn_derived__users__v1",
        destination_table="users_v1",
        dataset_id="mozilla_vpn_derived",
        project_id="moz-fx-data-shared-prod",
        owner="dthorn@mozilla.com",
        email=["dthorn@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        dag=dag,
    )

    mozilla_vpn_derived__vat_rates__v1 = gke_command(
        task_id="mozilla_vpn_derived__vat_rates__v1",
        command=[
            "python",
            "sql/moz-fx-data-shared-prod/mozilla_vpn_derived/vat_rates_v1/query.py",
        ]
        + [],
        docker_image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
        owner="dthorn@mozilla.com",
        email=["dthorn@mozilla.com", "telemetry-alerts@mozilla.com"],
        gcp_conn_id="google_cloud_airflow_gke",
        gke_project_id="moz-fx-data-airflow-gke-prod",
        gke_location="us-west1",
        gke_cluster_name="workloads-prod-v1",
        retry_delay=datetime.timedelta(seconds=300),
    )

    mozilla_vpn_derived__waitlist__v1 = bigquery_etl_query(
        task_id="mozilla_vpn_derived__waitlist__v1",
        destination_table="waitlist_v1",
        dataset_id="mozilla_vpn_derived",
        project_id="moz-fx-data-shared-prod",
        owner="dthorn@mozilla.com",
        email=["dthorn@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        dag=dag,
    )

    mozilla_vpn_external__devices__v1 = bigquery_etl_query(
        task_id="mozilla_vpn_external__devices__v1",
        destination_table="devices_v1",
        dataset_id="mozilla_vpn_external",
        project_id="moz-fx-data-shared-prod",
        owner="dthorn@mozilla.com",
        email=["dthorn@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=True,
        parameters=[
            "external_database_query:STRING:SELECT * FROM devices WHERE DATE(updated_at) = DATE '{{ds}}'"
        ],
        dag=dag,
    )

    mozilla_vpn_external__subscriptions__v1 = bigquery_etl_query(
        task_id="mozilla_vpn_external__subscriptions__v1",
        destination_table="subscriptions_v1",
        dataset_id="mozilla_vpn_external",
        project_id="moz-fx-data-shared-prod",
        owner="dthorn@mozilla.com",
        email=["dthorn@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=True,
        parameters=[
            "external_database_query:STRING:SELECT * FROM subscriptions WHERE DATE(updated_at) = DATE '{{ds}}'"
        ],
        dag=dag,
    )

    mozilla_vpn_external__users__v1 = bigquery_etl_query(
        task_id="mozilla_vpn_external__users__v1",
        destination_table="users_v1",
        dataset_id="mozilla_vpn_external",
        project_id="moz-fx-data-shared-prod",
        owner="dthorn@mozilla.com",
        email=["dthorn@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=True,
        parameters=[
            "external_database_query:STRING:SELECT * FROM users WHERE DATE(updated_at) = DATE '{{ds}}'"
        ],
        dag=dag,
    )

    mozilla_vpn_external__waitlist__v1 = bigquery_etl_query(
        task_id="mozilla_vpn_external__waitlist__v1",
        destination_table="waitlist_v1",
        dataset_id="mozilla_vpn_external",
        project_id="moz-fx-data-shared-prod",
        owner="dthorn@mozilla.com",
        email=["dthorn@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=True,
        parameters=[
            "external_database_query:STRING:SELECT * FROM vpn_waitlist WHERE DATE(updated_at) = DATE '{{ds}}'"
        ],
        dag=dag,
    )

    stripe_derived__customers__v1 = bigquery_etl_query(
        task_id="stripe_derived__customers__v1",
        destination_table="customers_v1",
        dataset_id="stripe_derived",
        project_id="moz-fx-data-shared-prod",
        owner="dthorn@mozilla.com",
        email=["dthorn@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        dag=dag,
    )

    stripe_derived__nonprod_customers__v1 = bigquery_etl_query(
        task_id="stripe_derived__nonprod_customers__v1",
        destination_table="nonprod_customers_v1",
        dataset_id="stripe_derived",
        project_id="moz-fx-data-shared-prod",
        owner="dthorn@mozilla.com",
        email=["dthorn@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        dag=dag,
    )

    stripe_derived__nonprod_plans__v1 = bigquery_etl_query(
        task_id="stripe_derived__nonprod_plans__v1",
        destination_table="nonprod_plans_v1",
        dataset_id="stripe_derived",
        project_id="moz-fx-data-shared-prod",
        owner="dthorn@mozilla.com",
        email=["dthorn@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        dag=dag,
    )

    stripe_derived__nonprod_products__v1 = bigquery_etl_query(
        task_id="stripe_derived__nonprod_products__v1",
        destination_table="nonprod_products_v1",
        dataset_id="stripe_derived",
        project_id="moz-fx-data-shared-prod",
        owner="dthorn@mozilla.com",
        email=["dthorn@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        dag=dag,
    )

    stripe_derived__nonprod_subscriptions__v1 = bigquery_etl_query(
        task_id="stripe_derived__nonprod_subscriptions__v1",
        destination_table="nonprod_subscriptions_v1",
        dataset_id="stripe_derived",
        project_id="moz-fx-data-shared-prod",
        owner="dthorn@mozilla.com",
        email=["dthorn@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        dag=dag,
    )

    stripe_derived__plans__v1 = bigquery_etl_query(
        task_id="stripe_derived__plans__v1",
        destination_table="plans_v1",
        dataset_id="stripe_derived",
        project_id="moz-fx-data-shared-prod",
        owner="dthorn@mozilla.com",
        email=["dthorn@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        dag=dag,
    )

    stripe_derived__products__v1 = bigquery_etl_query(
        task_id="stripe_derived__products__v1",
        destination_table="products_v1",
        dataset_id="stripe_derived",
        project_id="moz-fx-data-shared-prod",
        owner="dthorn@mozilla.com",
        email=["dthorn@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        dag=dag,
    )

    stripe_derived__subscriptions__v1 = bigquery_etl_query(
        task_id="stripe_derived__subscriptions__v1",
        destination_table="subscriptions_v1",
        dataset_id="stripe_derived",
        project_id="moz-fx-data-shared-prod",
        owner="dthorn@mozilla.com",
        email=["dthorn@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        dag=dag,
    )

    stripe_external__charges__v1 = bigquery_etl_query(
        task_id="stripe_external__charges__v1",
        destination_table="charges_v1",
        dataset_id="stripe_external",
        project_id="moz-fx-data-shared-prod",
        owner="dthorn@mozilla.com",
        email=["dthorn@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        parameters=["date:DATE:{{ds}}"],
        dag=dag,
    )

    stripe_external__credit_notes__v1 = bigquery_etl_query(
        task_id="stripe_external__credit_notes__v1",
        destination_table="credit_notes_v1",
        dataset_id="stripe_external",
        project_id="moz-fx-data-shared-prod",
        owner="dthorn@mozilla.com",
        email=["dthorn@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        parameters=["date:DATE:{{ds}}"],
        dag=dag,
    )

    stripe_external__customers__v1 = bigquery_etl_query(
        task_id="stripe_external__customers__v1",
        destination_table="customers_v1",
        dataset_id="stripe_external",
        project_id="moz-fx-data-shared-prod",
        owner="dthorn@mozilla.com",
        email=["dthorn@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        parameters=["date:DATE:{{ds}}"],
        dag=dag,
    )

    stripe_external__disputes__v1 = bigquery_etl_query(
        task_id="stripe_external__disputes__v1",
        destination_table="disputes_v1",
        dataset_id="stripe_external",
        project_id="moz-fx-data-shared-prod",
        owner="dthorn@mozilla.com",
        email=["dthorn@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        parameters=["date:DATE:{{ds}}"],
        dag=dag,
    )

    stripe_external__events__v1 = gke_command(
        task_id="stripe_external__events__v1",
        command=[
            "python",
            "sql/moz-fx-data-shared-prod/stripe_external/events_v1/query.py",
        ]
        + [
            "--date={{ ds }}",
            "--api-key={{ var.value.stripe_api_key }}",
            "--resource=Event",
            "--table=moz-fx-data-shared-prod.stripe_external.events_v1",
        ],
        docker_image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
        owner="dthorn@mozilla.com",
        email=["dthorn@mozilla.com", "telemetry-alerts@mozilla.com"],
        retry_delay=datetime.timedelta(seconds=300),
    )

    stripe_external__events_schema_check__v1 = gke_command(
        task_id="stripe_external__events_schema_check__v1",
        command=[
            "python",
            "sql/moz-fx-data-shared-prod/stripe_external/events_schema_check_v1/query.py",
        ]
        + [
            "--date={{ ds }}",
            "--api-key={{ var.value.stripe_api_key }}",
            "--resource=Event",
            "--format-resources",
            "--strict-schema",
            "--quiet",
        ],
        docker_image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
        owner="dthorn@mozilla.com",
        email=["dthorn@mozilla.com", "telemetry-alerts@mozilla.com"],
        retry_delay=datetime.timedelta(seconds=300),
    )

    stripe_external__invoices__v1 = bigquery_etl_query(
        task_id="stripe_external__invoices__v1",
        destination_table="invoices_v1",
        dataset_id="stripe_external",
        project_id="moz-fx-data-shared-prod",
        owner="dthorn@mozilla.com",
        email=["dthorn@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        parameters=["date:DATE:{{ds}}"],
        dag=dag,
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
        owner="dthorn@mozilla.com",
        email=["dthorn@mozilla.com", "telemetry-alerts@mozilla.com"],
        retry_delay=datetime.timedelta(seconds=1800),
        retries=47,
        email_on_retry=False,
    )

    stripe_external__nonprod_charges__v1 = bigquery_etl_query(
        task_id="stripe_external__nonprod_charges__v1",
        destination_table="nonprod_charges_v1",
        dataset_id="stripe_external",
        project_id="moz-fx-data-shared-prod",
        owner="dthorn@mozilla.com",
        email=["dthorn@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        parameters=["date:DATE:{{ds}}"],
        dag=dag,
    )

    stripe_external__nonprod_customers__v1 = bigquery_etl_query(
        task_id="stripe_external__nonprod_customers__v1",
        destination_table="nonprod_customers_v1",
        dataset_id="stripe_external",
        project_id="moz-fx-data-shared-prod",
        owner="dthorn@mozilla.com",
        email=["dthorn@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        parameters=["date:DATE:{{ds}}"],
        dag=dag,
    )

    stripe_external__nonprod_events__v1 = gke_command(
        task_id="stripe_external__nonprod_events__v1",
        command=[
            "python",
            "sql/moz-fx-data-shared-prod/stripe_external/nonprod_events_v1/query.py",
        ]
        + [
            "--date={{ ds }}",
            "--api-key={{ var.value.nonprod_stripe_api_key }}",
            "--resource=Event",
            "--table=moz-fx-data-shared-prod.stripe_external.nonprod_events_v1",
            "--allow-empty",
        ],
        docker_image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
        owner="dthorn@mozilla.com",
        email=["dthorn@mozilla.com", "telemetry-alerts@mozilla.com"],
        retry_delay=datetime.timedelta(seconds=300),
    )

    stripe_external__nonprod_events_schema_check__v1 = gke_command(
        task_id="stripe_external__nonprod_events_schema_check__v1",
        command=[
            "python",
            "sql/moz-fx-data-shared-prod/stripe_external/nonprod_events_schema_check_v1/query.py",
        ]
        + [
            "--date={{ ds }}",
            "--api-key={{ var.value.nonprod_stripe_api_key }}",
            "--resource=Event",
            "--format-resources",
            "--strict-schema",
            "--quiet",
            "--allow-empty",
        ],
        docker_image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
        owner="dthorn@mozilla.com",
        email=["dthorn@mozilla.com", "telemetry-alerts@mozilla.com"],
        retry_delay=datetime.timedelta(seconds=300),
    )

    stripe_external__nonprod_invoices__v1 = bigquery_etl_query(
        task_id="stripe_external__nonprod_invoices__v1",
        destination_table="nonprod_invoices_v1",
        dataset_id="stripe_external",
        project_id="moz-fx-data-shared-prod",
        owner="dthorn@mozilla.com",
        email=["dthorn@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        parameters=["date:DATE:{{ds}}"],
        dag=dag,
    )

    stripe_external__nonprod_plans__v1 = bigquery_etl_query(
        task_id="stripe_external__nonprod_plans__v1",
        destination_table="nonprod_plans_v1",
        dataset_id="stripe_external",
        project_id="moz-fx-data-shared-prod",
        owner="dthorn@mozilla.com",
        email=["dthorn@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        parameters=["date:DATE:{{ds}}"],
        dag=dag,
    )

    stripe_external__nonprod_products__v1 = bigquery_etl_query(
        task_id="stripe_external__nonprod_products__v1",
        destination_table="nonprod_products_v1",
        dataset_id="stripe_external",
        project_id="moz-fx-data-shared-prod",
        owner="dthorn@mozilla.com",
        email=["dthorn@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        parameters=["date:DATE:{{ds}}"],
        dag=dag,
    )

    stripe_external__nonprod_promotion_codes__v1 = bigquery_etl_query(
        task_id="stripe_external__nonprod_promotion_codes__v1",
        destination_table="nonprod_promotion_codes_v1",
        dataset_id="stripe_external",
        project_id="moz-fx-data-shared-prod",
        owner="dthorn@mozilla.com",
        email=["dthorn@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        parameters=["date:DATE:{{ds}}"],
        dag=dag,
    )

    stripe_external__nonprod_subscriptions__v1 = bigquery_etl_query(
        task_id="stripe_external__nonprod_subscriptions__v1",
        destination_table="nonprod_subscriptions_v1",
        dataset_id="stripe_external",
        project_id="moz-fx-data-shared-prod",
        owner="dthorn@mozilla.com",
        email=["dthorn@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        parameters=["date:DATE:{{ds}}"],
        dag=dag,
    )

    stripe_external__payment_intents__v1 = bigquery_etl_query(
        task_id="stripe_external__payment_intents__v1",
        destination_table="payment_intents_v1",
        dataset_id="stripe_external",
        project_id="moz-fx-data-shared-prod",
        owner="dthorn@mozilla.com",
        email=["dthorn@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        parameters=["date:DATE:{{ds}}"],
        dag=dag,
    )

    stripe_external__payouts__v1 = bigquery_etl_query(
        task_id="stripe_external__payouts__v1",
        destination_table="payouts_v1",
        dataset_id="stripe_external",
        project_id="moz-fx-data-shared-prod",
        owner="dthorn@mozilla.com",
        email=["dthorn@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        parameters=["date:DATE:{{ds}}"],
        dag=dag,
    )

    stripe_external__plans__v1 = bigquery_etl_query(
        task_id="stripe_external__plans__v1",
        destination_table="plans_v1",
        dataset_id="stripe_external",
        project_id="moz-fx-data-shared-prod",
        owner="dthorn@mozilla.com",
        email=["dthorn@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        parameters=["date:DATE:{{ds}}"],
        dag=dag,
    )

    stripe_external__prices__v1 = bigquery_etl_query(
        task_id="stripe_external__prices__v1",
        destination_table="prices_v1",
        dataset_id="stripe_external",
        project_id="moz-fx-data-shared-prod",
        owner="dthorn@mozilla.com",
        email=["dthorn@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        parameters=["date:DATE:{{ds}}"],
        dag=dag,
    )

    stripe_external__products__v1 = bigquery_etl_query(
        task_id="stripe_external__products__v1",
        destination_table="products_v1",
        dataset_id="stripe_external",
        project_id="moz-fx-data-shared-prod",
        owner="dthorn@mozilla.com",
        email=["dthorn@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        parameters=["date:DATE:{{ds}}"],
        dag=dag,
    )

    stripe_external__promotion_codes__v1 = bigquery_etl_query(
        task_id="stripe_external__promotion_codes__v1",
        destination_table="promotion_codes_v1",
        dataset_id="stripe_external",
        project_id="moz-fx-data-shared-prod",
        owner="dthorn@mozilla.com",
        email=["dthorn@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        parameters=["date:DATE:{{ds}}"],
        dag=dag,
    )

    stripe_external__setup_intents__v1 = bigquery_etl_query(
        task_id="stripe_external__setup_intents__v1",
        destination_table="setup_intents_v1",
        dataset_id="stripe_external",
        project_id="moz-fx-data-shared-prod",
        owner="dthorn@mozilla.com",
        email=["dthorn@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        parameters=["date:DATE:{{ds}}"],
        dag=dag,
    )

    stripe_external__subscriptions__v1 = bigquery_etl_query(
        task_id="stripe_external__subscriptions__v1",
        destination_table="subscriptions_v1",
        dataset_id="stripe_external",
        project_id="moz-fx-data-shared-prod",
        owner="dthorn@mozilla.com",
        email=["dthorn@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        parameters=["date:DATE:{{ds}}"],
        dag=dag,
    )

    mozilla_vpn_derived__active_subscriptions__v1.set_upstream(
        mozilla_vpn_derived__all_subscriptions__v1
    )

    mozilla_vpn_derived__all_subscriptions__v1.set_upstream(
        mozilla_vpn_derived__fxa_attribution__v1
    )

    mozilla_vpn_derived__all_subscriptions__v1.set_upstream(
        mozilla_vpn_derived__subscriptions__v1
    )

    mozilla_vpn_derived__all_subscriptions__v1.set_upstream(
        mozilla_vpn_derived__users__v1
    )

    mozilla_vpn_derived__all_subscriptions__v1.set_upstream(
        stripe_derived__customers__v1
    )

    mozilla_vpn_derived__all_subscriptions__v1.set_upstream(stripe_derived__plans__v1)

    mozilla_vpn_derived__all_subscriptions__v1.set_upstream(
        stripe_derived__products__v1
    )

    mozilla_vpn_derived__all_subscriptions__v1.set_upstream(
        stripe_derived__subscriptions__v1
    )

    mozilla_vpn_derived__all_subscriptions__v1.set_upstream(
        stripe_external__charges__v1
    )

    mozilla_vpn_derived__all_subscriptions__v1.set_upstream(
        stripe_external__invoices__v1
    )

    mozilla_vpn_derived__all_subscriptions__v1.set_upstream(
        stripe_external__promotion_codes__v1
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

    wait_for_firefox_accounts_derived__fxa_auth_events__v1 = (
        ExternalTaskCompletedSensor(
            task_id="wait_for_firefox_accounts_derived__fxa_auth_events__v1",
            external_dag_id="bqetl_fxa_events",
            external_task_id="firefox_accounts_derived__fxa_auth_events__v1",
            execution_delta=datetime.timedelta(seconds=900),
            check_existence=True,
            mode="reschedule",
            pool="DATA_ENG_EXTERNALTASKSENSOR",
        )
    )

    mozilla_vpn_derived__funnel_product_page_to_subscribed__v1.set_upstream(
        wait_for_firefox_accounts_derived__fxa_auth_events__v1
    )
    wait_for_firefox_accounts_derived__fxa_content_events__v1 = (
        ExternalTaskCompletedSensor(
            task_id="wait_for_firefox_accounts_derived__fxa_content_events__v1",
            external_dag_id="bqetl_fxa_events",
            external_task_id="firefox_accounts_derived__fxa_content_events__v1",
            execution_delta=datetime.timedelta(seconds=900),
            check_existence=True,
            mode="reschedule",
            pool="DATA_ENG_EXTERNALTASKSENSOR",
        )
    )

    mozilla_vpn_derived__funnel_product_page_to_subscribed__v1.set_upstream(
        wait_for_firefox_accounts_derived__fxa_content_events__v1
    )
    wait_for_firefox_accounts_derived__fxa_stdout_events__v1 = (
        ExternalTaskCompletedSensor(
            task_id="wait_for_firefox_accounts_derived__fxa_stdout_events__v1",
            external_dag_id="bqetl_fxa_events",
            external_task_id="firefox_accounts_derived__fxa_stdout_events__v1",
            execution_delta=datetime.timedelta(seconds=900),
            check_existence=True,
            mode="reschedule",
            pool="DATA_ENG_EXTERNALTASKSENSOR",
        )
    )

    mozilla_vpn_derived__funnel_product_page_to_subscribed__v1.set_upstream(
        wait_for_firefox_accounts_derived__fxa_stdout_events__v1
    )

    mozilla_vpn_derived__funnel_product_page_to_subscribed__v1.set_upstream(
        stripe_derived__plans__v1
    )

    mozilla_vpn_derived__fxa_attribution__v1.set_upstream(
        wait_for_firefox_accounts_derived__fxa_auth_events__v1
    )
    mozilla_vpn_derived__fxa_attribution__v1.set_upstream(
        wait_for_firefox_accounts_derived__fxa_content_events__v1
    )
    mozilla_vpn_derived__fxa_attribution__v1.set_upstream(
        wait_for_firefox_accounts_derived__fxa_stdout_events__v1
    )

    mozilla_vpn_derived__login_flows__v1.set_upstream(
        wait_for_firefox_accounts_derived__fxa_auth_events__v1
    )
    mozilla_vpn_derived__login_flows__v1.set_upstream(
        wait_for_firefox_accounts_derived__fxa_content_events__v1
    )

    mozilla_vpn_derived__subscription_events__v1.set_upstream(
        mozilla_vpn_derived__all_subscriptions__v1
    )

    mozilla_vpn_derived__subscriptions__v1.set_upstream(
        mozilla_vpn_external__subscriptions__v1
    )

    mozilla_vpn_derived__users__v1.set_upstream(mozilla_vpn_external__users__v1)

    mozilla_vpn_derived__waitlist__v1.set_upstream(mozilla_vpn_external__waitlist__v1)

    stripe_derived__customers__v1.set_upstream(stripe_external__customers__v1)

    stripe_derived__nonprod_customers__v1.set_upstream(
        stripe_external__nonprod_customers__v1
    )

    stripe_derived__nonprod_plans__v1.set_upstream(stripe_external__nonprod_plans__v1)

    stripe_derived__nonprod_products__v1.set_upstream(
        stripe_external__nonprod_products__v1
    )

    stripe_derived__nonprod_subscriptions__v1.set_upstream(
        stripe_external__nonprod_subscriptions__v1
    )

    stripe_derived__plans__v1.set_upstream(stripe_external__plans__v1)

    stripe_derived__products__v1.set_upstream(stripe_external__products__v1)

    stripe_derived__subscriptions__v1.set_upstream(stripe_external__subscriptions__v1)

    stripe_external__charges__v1.set_upstream(stripe_external__events__v1)

    stripe_external__credit_notes__v1.set_upstream(stripe_external__events__v1)

    stripe_external__customers__v1.set_upstream(stripe_external__events__v1)

    stripe_external__disputes__v1.set_upstream(stripe_external__events__v1)

    stripe_external__invoices__v1.set_upstream(stripe_external__events__v1)

    stripe_external__nonprod_charges__v1.set_upstream(
        stripe_external__nonprod_events__v1
    )

    stripe_external__nonprod_customers__v1.set_upstream(
        stripe_external__nonprod_events__v1
    )

    stripe_external__nonprod_invoices__v1.set_upstream(
        stripe_external__nonprod_events__v1
    )

    stripe_external__nonprod_plans__v1.set_upstream(stripe_external__nonprod_events__v1)

    stripe_external__nonprod_products__v1.set_upstream(
        stripe_external__nonprod_events__v1
    )

    stripe_external__nonprod_promotion_codes__v1.set_upstream(
        stripe_external__nonprod_events__v1
    )

    stripe_external__nonprod_subscriptions__v1.set_upstream(
        stripe_external__nonprod_events__v1
    )

    stripe_external__payment_intents__v1.set_upstream(stripe_external__events__v1)

    stripe_external__payouts__v1.set_upstream(stripe_external__events__v1)

    stripe_external__plans__v1.set_upstream(stripe_external__events__v1)

    stripe_external__prices__v1.set_upstream(stripe_external__events__v1)

    stripe_external__products__v1.set_upstream(stripe_external__events__v1)

    stripe_external__promotion_codes__v1.set_upstream(stripe_external__events__v1)

    stripe_external__setup_intents__v1.set_upstream(stripe_external__events__v1)

    stripe_external__subscriptions__v1.set_upstream(stripe_external__events__v1)
