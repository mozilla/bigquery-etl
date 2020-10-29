# Generated via https://github.com/mozilla/bigquery-etl/blob/master/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from airflow.operators.sensors import ExternalTaskSensor
import datetime
from utils.gcp import bigquery_etl_query

default_args = {
    "owner": "dthorn@mozilla.com",
    "start_date": datetime.datetime(2020, 10, 5, 0, 0),
    "email": ["telemetry-alerts@mozilla.com", "dthorn@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=300),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
}

with DAG("bqetl_stripe", default_args=default_args, schedule_interval="@daily") as dag:

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

    stripe_derived__customers__v1.set_upstream(stripe_external__customers__v1)

    stripe_derived__plans__v1.set_upstream(stripe_external__plans__v1)

    stripe_derived__products__v1.set_upstream(stripe_external__products__v1)

    stripe_derived__subscriptions__v1.set_upstream(stripe_external__subscriptions__v1)

    wait_for_stripe_stripe_import_events = ExternalTaskSensor(
        task_id="wait_for_stripe_stripe_import_events",
        external_dag_id="stripe",
        external_task_id="stripe_import_events",
        check_existence=True,
        mode="reschedule",
        pool="DATA_ENG_EXTERNALTASKSENSOR",
        dag=dag,
    )

    stripe_external__charges__v1.set_upstream(wait_for_stripe_stripe_import_events)

    stripe_external__credit_notes__v1.set_upstream(wait_for_stripe_stripe_import_events)

    stripe_external__customers__v1.set_upstream(wait_for_stripe_stripe_import_events)

    stripe_external__disputes__v1.set_upstream(wait_for_stripe_stripe_import_events)

    stripe_external__invoices__v1.set_upstream(wait_for_stripe_stripe_import_events)

    stripe_external__payment_intents__v1.set_upstream(
        wait_for_stripe_stripe_import_events
    )

    stripe_external__payouts__v1.set_upstream(wait_for_stripe_stripe_import_events)

    stripe_external__plans__v1.set_upstream(wait_for_stripe_stripe_import_events)

    stripe_external__prices__v1.set_upstream(wait_for_stripe_stripe_import_events)

    stripe_external__products__v1.set_upstream(wait_for_stripe_stripe_import_events)

    stripe_external__setup_intents__v1.set_upstream(
        wait_for_stripe_stripe_import_events
    )

    stripe_external__subscriptions__v1.set_upstream(
        wait_for_stripe_stripe_import_events
    )
