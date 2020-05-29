# Generated via query_scheduling/generate_airflow_dags

from airflow import DAG
from airflow.operators.sensors import ExternalTaskSensor
import datetime
from utils.gcp import gke_command

default_args = {
    "owner": "ascholtz@mozilla.com",
    "start_date": datetime.datetime(2020, 4, 14, 0, 0),
    "email": ["telemetry-alerts@mozilla.com", "ascholtz@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
}

with DAG(
    "bqetl_public_data", default_args=default_args, schedule_interval="@daily"
) as dag:
    docker_image = "mozilla/bigquery-etl:latest"

    export_public_data_telemetry_derived__ssl_ratios__v1 = gke_command(
        task_id="export_public_data_telemetry_derived__ssl_ratios__v1",
        command=["/script/publish_public_data_json"],
        arguments=["--query_file=sql/telemetry_derived/ssl_ratios_v1/query.sql"]
        + ["--destination_table=ssl_ratios$"]
        + ["--dataset_id=telemetry_derived"]
        + ["--project_id=moz-fx-data-shared-prod"]
        + ["--parameter=submission_date:DATE:"],
        docker_image=docker_image,
        dag=dag,
    )

    wait_for_telemetry_derived__ssl_ratios__v1 = ExternalTaskSensor(
        task_id="wait_for_telemetry_derived__ssl_ratios__v1",
        external_dag_id="bqetl_ssl_ratios",
        external_task_id="telemetry_derived__ssl_ratios__v1",
    )

    export_public_data_telemetry_derived__ssl_ratios__v1.set_upstream(
        wait_for_telemetry_derived__ssl_ratios__v1
    )

    public_data_gcs_metadata = gke_command(
        task_id="public_data_gcs_metadata",
        command=["script/publish_public_data_gcs_metadata"],
        docker_image=docker_image,
        dag=dag,
    )

    public_data_gcs_metadata.set_upstream(
        [export_public_data_telemetry_derived__ssl_ratios__v1,]
    )
