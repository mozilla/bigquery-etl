# Generated via https://github.com/mozilla/bigquery-etl/blob/main/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from airflow.operators.sensors import ExternalTaskSensor
import datetime
from utils.gcp import bigquery_etl_query, gke_command

docs = """
### bqetl_mozilla_vpn

Built from bigquery-etl repo, [`dags/bqetl_mozilla_vpn.py`](https://github.com/mozilla/bigquery-etl/blob/main/dags/bqetl_mozilla_vpn.py)

#### Description

Daily extracts from the Mozilla VPN operational DB to BigQuery
as well as derived tables based on that data.

Depends on `bqetl_fxa_events`, so is scheduled to run a bit
after that one.

#### Owner

dthorn@mozilla.com
"""


default_args = {
    "owner": "dthorn@mozilla.com",
    "start_date": datetime.datetime(2020, 10, 8, 0, 0),
    "end_date": None,
    "email": ["telemetry-alerts@mozilla.com", "dthorn@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
}

with DAG(
    "bqetl_mozilla_vpn",
    default_args=default_args,
    schedule_interval="45 1 * * *",
    doc_md=docs,
) as dag:

    mozilla_vpn_derived__active_subscriptions__v1 = bigquery_etl_query(
        task_id="mozilla_vpn_derived__active_subscriptions__v1",
        destination_table="active_subscriptions_v1",
        dataset_id="mozilla_vpn_derived",
        project_id="moz-fx-data-shared-prod",
        owner="dthorn@mozilla.com",
        email=["dthorn@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
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

    mozilla_vpn_derived__new_subscriptions__v1 = bigquery_etl_query(
        task_id="mozilla_vpn_derived__new_subscriptions__v1",
        destination_table="new_subscriptions_v1",
        dataset_id="mozilla_vpn_derived",
        project_id="moz-fx-data-shared-prod",
        owner="dthorn@mozilla.com",
        email=["dthorn@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
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

    mozilla_vpn_derived__retention_by_subscription__v1 = bigquery_etl_query(
        task_id="mozilla_vpn_derived__retention_by_subscription__v1",
        destination_table="retention_by_subscription_v1",
        dataset_id="mozilla_vpn_derived",
        project_id="moz-fx-data-shared-prod",
        owner="dthorn@mozilla.com",
        email=["dthorn@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
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
        owner="amiyaguchi@mozilla.com",
        email=[
            "amiyaguchi@mozilla.com",
            "dthorn@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
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
        owner="amiyaguchi@mozilla.com",
        email=[
            "amiyaguchi@mozilla.com",
            "dthorn@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
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
        owner="amiyaguchi@mozilla.com",
        email=[
            "amiyaguchi@mozilla.com",
            "dthorn@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
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
        owner="amiyaguchi@mozilla.com",
        email=[
            "amiyaguchi@mozilla.com",
            "dthorn@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
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

    mozilla_vpn_derived__active_subscriptions__v1.set_upstream(
        mozilla_vpn_derived__all_subscriptions__v1
    )

    mozilla_vpn_derived__all_subscriptions__v1.set_upstream(
        mozilla_vpn_derived__subscriptions__v1
    )

    mozilla_vpn_derived__all_subscriptions__v1.set_upstream(
        mozilla_vpn_derived__users__v1
    )
    wait_for_stripe_derived__customers__v1 = ExternalTaskSensor(
        task_id="wait_for_stripe_derived__customers__v1",
        external_dag_id="bqetl_stripe",
        external_task_id="stripe_derived__customers__v1",
        execution_delta=datetime.timedelta(seconds=900),
        check_existence=True,
        mode="reschedule",
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    mozilla_vpn_derived__all_subscriptions__v1.set_upstream(
        wait_for_stripe_derived__customers__v1
    )
    wait_for_stripe_derived__plans__v1 = ExternalTaskSensor(
        task_id="wait_for_stripe_derived__plans__v1",
        external_dag_id="bqetl_stripe",
        external_task_id="stripe_derived__plans__v1",
        execution_delta=datetime.timedelta(seconds=900),
        check_existence=True,
        mode="reschedule",
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    mozilla_vpn_derived__all_subscriptions__v1.set_upstream(
        wait_for_stripe_derived__plans__v1
    )
    wait_for_stripe_derived__products__v1 = ExternalTaskSensor(
        task_id="wait_for_stripe_derived__products__v1",
        external_dag_id="bqetl_stripe",
        external_task_id="stripe_derived__products__v1",
        execution_delta=datetime.timedelta(seconds=900),
        check_existence=True,
        mode="reschedule",
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    mozilla_vpn_derived__all_subscriptions__v1.set_upstream(
        wait_for_stripe_derived__products__v1
    )
    wait_for_stripe_derived__subscriptions__v1 = ExternalTaskSensor(
        task_id="wait_for_stripe_derived__subscriptions__v1",
        external_dag_id="bqetl_stripe",
        external_task_id="stripe_derived__subscriptions__v1",
        execution_delta=datetime.timedelta(seconds=900),
        check_existence=True,
        mode="reschedule",
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    mozilla_vpn_derived__all_subscriptions__v1.set_upstream(
        wait_for_stripe_derived__subscriptions__v1
    )
    wait_for_stripe_external__charges__v1 = ExternalTaskSensor(
        task_id="wait_for_stripe_external__charges__v1",
        external_dag_id="bqetl_stripe",
        external_task_id="stripe_external__charges__v1",
        execution_delta=datetime.timedelta(seconds=900),
        check_existence=True,
        mode="reschedule",
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    mozilla_vpn_derived__all_subscriptions__v1.set_upstream(
        wait_for_stripe_external__charges__v1
    )
    wait_for_stripe_external__fxa_pay_setup_complete__v1 = ExternalTaskSensor(
        task_id="wait_for_stripe_external__fxa_pay_setup_complete__v1",
        external_dag_id="bqetl_stripe",
        external_task_id="stripe_external__fxa_pay_setup_complete__v1",
        execution_delta=datetime.timedelta(seconds=900),
        check_existence=True,
        mode="reschedule",
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    mozilla_vpn_derived__all_subscriptions__v1.set_upstream(
        wait_for_stripe_external__fxa_pay_setup_complete__v1
    )
    wait_for_stripe_external__invoices__v1 = ExternalTaskSensor(
        task_id="wait_for_stripe_external__invoices__v1",
        external_dag_id="bqetl_stripe",
        external_task_id="stripe_external__invoices__v1",
        execution_delta=datetime.timedelta(seconds=900),
        check_existence=True,
        mode="reschedule",
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    mozilla_vpn_derived__all_subscriptions__v1.set_upstream(
        wait_for_stripe_external__invoices__v1
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

    wait_for_firefox_accounts_derived__fxa_auth_events__v1 = ExternalTaskSensor(
        task_id="wait_for_firefox_accounts_derived__fxa_auth_events__v1",
        external_dag_id="bqetl_fxa_events",
        external_task_id="firefox_accounts_derived__fxa_auth_events__v1",
        execution_delta=datetime.timedelta(seconds=900),
        check_existence=True,
        mode="reschedule",
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    mozilla_vpn_derived__login_flows__v1.set_upstream(
        wait_for_firefox_accounts_derived__fxa_auth_events__v1
    )
    wait_for_firefox_accounts_derived__fxa_content_events__v1 = ExternalTaskSensor(
        task_id="wait_for_firefox_accounts_derived__fxa_content_events__v1",
        external_dag_id="bqetl_fxa_events",
        external_task_id="firefox_accounts_derived__fxa_content_events__v1",
        execution_delta=datetime.timedelta(seconds=900),
        check_existence=True,
        mode="reschedule",
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    mozilla_vpn_derived__login_flows__v1.set_upstream(
        wait_for_firefox_accounts_derived__fxa_content_events__v1
    )

    mozilla_vpn_derived__new_subscriptions__v1.set_upstream(
        mozilla_vpn_derived__all_subscriptions__v1
    )

    mozilla_vpn_derived__retention_by_subscription__v1.set_upstream(
        mozilla_vpn_derived__all_subscriptions__v1
    )

    mozilla_vpn_derived__subscriptions__v1.set_upstream(
        mozilla_vpn_external__subscriptions__v1
    )

    mozilla_vpn_derived__users__v1.set_upstream(mozilla_vpn_external__users__v1)

    mozilla_vpn_derived__waitlist__v1.set_upstream(mozilla_vpn_external__waitlist__v1)
