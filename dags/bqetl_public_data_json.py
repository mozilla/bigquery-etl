# Generated via https://github.com/mozilla/bigquery-etl/blob/master/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from airflow.operators.sensors import ExternalTaskSensor
import datetime
from operators.gcp_container_operator import GKEPodOperator
from utils.gcp import gke_command

docs = """
### bqetl_public_data_json

Built from bigquery-etl repo, [`dags/bqetl_public_data_json.py`](https://github.com/mozilla/bigquery-etl/blob/master/dags/bqetl_public_data_json.py)

#### Description

The DAG exports query data marked as public to GCS
#### Owner

ascholtz@mozilla.com
"""


default_args = {
    "owner": "ascholtz@mozilla.com",
    "start_date": datetime.datetime(2020, 4, 14, 0, 0),
    "end_date": None,
    "email": ["telemetry-alerts@mozilla.com", "ascholtz@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
}

with DAG(
    "bqetl_public_data_json",
    default_args=default_args,
    schedule_interval="0 4 * * *",
    doc_md=docs,
) as dag:
    docker_image = "mozilla/bigquery-etl:latest"

    export_public_data_json_mozregression_aggregates__v1 = GKEPodOperator(
        task_id="export_public_data_json_mozregression_aggregates__v1",
        name="export_public_data_json_mozregression_aggregates__v1",
        arguments=["script/publish_public_data_json"]
        + [
            "--query_file=sql/moz-fx-data-shared-prod/org_mozilla_mozregression_derived/mozregression_aggregates_v1/query.sql"
        ]
        + ["--destination_table=mozregression_aggregates${{ds_nodash}}"]
        + ["--dataset_id=org_mozilla_mozregression_derived"]
        + ["--project_id=moz-fx-data-shared-prod"]
        + ["--parameter=submission_date:DATE:{{ds}}"],
        image=docker_image,
        dag=dag,
    )

    export_public_data_json_telemetry_derived__ssl_ratios__v1 = GKEPodOperator(
        task_id="export_public_data_json_telemetry_derived__ssl_ratios__v1",
        name="export_public_data_json_telemetry_derived__ssl_ratios__v1",
        arguments=["script/publish_public_data_json"]
        + [
            "--query_file=sql/moz-fx-data-shared-prod/telemetry_derived/ssl_ratios_v1/query.sql"
        ]
        + ["--destination_table=ssl_ratios${{ds_nodash}}"]
        + ["--dataset_id=telemetry_derived"]
        + ["--project_id=moz-fx-data-shared-prod"]
        + ["--parameter=submission_date:DATE:{{ds}}"],
        image=docker_image,
        dag=dag,
    )

    wait_for_mozregression_aggregates__v1 = ExternalTaskSensor(
        task_id="wait_for_mozregression_aggregates__v1",
        external_dag_id="bqetl_internal_tooling",
        external_task_id="mozregression_aggregates__v1",
        check_existence=True,
        mode="reschedule",
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    export_public_data_json_mozregression_aggregates__v1.set_upstream(
        wait_for_mozregression_aggregates__v1
    )

    wait_for_telemetry_derived__ssl_ratios__v1 = ExternalTaskSensor(
        task_id="wait_for_telemetry_derived__ssl_ratios__v1",
        external_dag_id="bqetl_ssl_ratios",
        external_task_id="telemetry_derived__ssl_ratios__v1",
        execution_delta=datetime.timedelta(seconds=7200),
        check_existence=True,
        mode="reschedule",
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    export_public_data_json_telemetry_derived__ssl_ratios__v1.set_upstream(
        wait_for_telemetry_derived__ssl_ratios__v1
    )

    public_data_gcs_metadata = gke_command(
        task_id="public_data_gcs_metadata",
        command=["script/publish_public_data_gcs_metadata"],
        docker_image=docker_image,
        dag=dag,
    )

    public_data_gcs_metadata.set_upstream(
        [
            export_public_data_json_mozregression_aggregates__v1,
            export_public_data_json_telemetry_derived__ssl_ratios__v1,
        ]
    )
