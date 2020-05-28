# Generated via query_scheduling/generate_airflow_dags

from airflow import DAG
from airflow.operators.sensors import ExternalTaskSensor
import datetime
from utils.gcp import bigquery_etl_query

default_args = {
    "owner": "jklukas@mozilla.com",
    "start_date": datetime.datetime(2020, 5, 12, 0, 0),
    "email": ["telemetry-alerts@mozilla.com", "jklukas@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=600),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
}

with DAG(
    "bqetl_kpi_dashboard", default_args=default_args, schedule_interval="45 15 * * *"
) as dag:

    telemetry_derived__smoot_usage_new_profiles__v2 = bigquery_etl_query(
        task_id="telemetry_derived__smoot_usage_new_profiles__v2",
        destination_table="smoot_usage_new_profiles_v2",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jklukas@mozilla.com",
        email=["jklukas@mozilla.com"],
        depends_on_past=False,
        dag=dag,
    )

    telemetry_derived__smoot_usage_new_profiles_compressed__v2 = bigquery_etl_query(
        task_id="telemetry_derived__smoot_usage_new_profiles_compressed__v2",
        destination_table="smoot_usage_new_profiles_compressed_v2",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jklukas@mozilla.com",
        email=["jklukas@mozilla.com"],
        depends_on_past=False,
        dag=dag,
    )

    telemetry__firefox_kpi_dashboard__v1 = bigquery_etl_query(
        task_id="telemetry__firefox_kpi_dashboard__v1",
        destination_table="firefox_kpi_dashboard_v1",
        dataset_id="telemetry",
        project_id="moz-fx-data-shared-prod",
        owner="jklukas@mozilla.com",
        email=["jklukas@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        dag=dag,
    )

    wait_for_smoot_usage_desktop_v2 = ExternalTaskSensor(
        task_id="wait_for_smoot_usage_desktop_v2",
        external_dag_id="main_summary",
        external_task_id="smoot_usage_desktop_v2",
    )

    telemetry_derived__smoot_usage_new_profiles__v2.set_upstream(
        wait_for_smoot_usage_desktop_v2
    )

    wait_for_smoot_usage_fxa_v2 = ExternalTaskSensor(
        task_id="wait_for_smoot_usage_fxa_v2",
        external_dag_id="fxa_events",
        external_task_id="smoot_usage_fxa_v2",
    )

    telemetry_derived__smoot_usage_new_profiles__v2.set_upstream(
        wait_for_smoot_usage_fxa_v2
    )

    wait_for_smoot_usage_nondesktop_v2 = ExternalTaskSensor(
        task_id="wait_for_smoot_usage_nondesktop_v2",
        external_dag_id="copy_deduplicate",
        external_task_id="smoot_usage_nondesktop_v2",
    )

    telemetry_derived__smoot_usage_new_profiles__v2.set_upstream(
        wait_for_smoot_usage_nondesktop_v2
    )

    telemetry_derived__smoot_usage_new_profiles_compressed__v2.set_upstream(
        telemetry_derived__smoot_usage_new_profiles__v2
    )

    wait_for_firefox_accounts_exact_mau28_raw = ExternalTaskSensor(
        task_id="wait_for_firefox_accounts_exact_mau28_raw",
        external_dag_id="fxa_events",
        external_task_id="firefox_accounts_exact_mau28_raw",
    )

    telemetry__firefox_kpi_dashboard__v1.set_upstream(
        wait_for_firefox_accounts_exact_mau28_raw
    )

    wait_for_exact_mau_by_dimensions = ExternalTaskSensor(
        task_id="wait_for_exact_mau_by_dimensions",
        external_dag_id="main_summary",
        external_task_id="exact_mau_by_dimensions",
    )

    telemetry__firefox_kpi_dashboard__v1.set_upstream(wait_for_exact_mau_by_dimensions)

    wait_for_firefox_nondesktop_exact_mau28 = ExternalTaskSensor(
        task_id="wait_for_firefox_nondesktop_exact_mau28",
        external_dag_id="copy_deduplicate",
        external_task_id="firefox_nondesktop_exact_mau28",
    )

    telemetry__firefox_kpi_dashboard__v1.set_upstream(
        wait_for_firefox_nondesktop_exact_mau28
    )
