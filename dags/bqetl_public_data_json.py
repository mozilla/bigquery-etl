# Generated via https://github.com/mozilla/bigquery-etl/blob/main/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from airflow.utils.state import State
import datetime
from operators.gcp_container_operator import GKEPodOperator
from airflow.sensors.external_task import ExternalTaskSensor
from utils.constants import ALLOWED_STATES, FAILED_STATES
from utils.gcp import gke_command

docs = """
### bqetl_public_data_json

Built from bigquery-etl repo, [`dags/bqetl_public_data_json.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_public_data_json.py)

#### Description

Daily exports of query data marked as public to GCS.

Depends on the results of several upstream DAGs, the latest of which
runs at 04:00 UTC.

#### Owner

ascholtz@mozilla.com

#### Tags

* impact/tier_3
* repo/bigquery-etl
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

tags = ["impact/tier_3", "repo/bigquery-etl"]

with DAG(
    "bqetl_public_data_json",
    default_args=default_args,
    schedule_interval="0 5 * * *",
    doc_md=docs,
    tags=tags,
) as dag:
    docker_image = "gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest"

    export_public_data_json_client_probe_processes__v1 = GKEPodOperator(
        task_id="export_public_data_json_client_probe_processes__v1",
        name="export_public_data_json_client_probe_processes__v1",
        arguments=["script/publish_public_data_json"]
        + [
            "--query_file=sql/moz-fx-data-shared-prod/telemetry_derived/client_probe_processes_v1/query.sql"
        ]
        + ["--destination_table=client_probe_processes"]
        + ["--dataset_id=telemetry_derived"]
        + ["--project_id=moz-fx-data-shared-prod"],
        image=docker_image,
    )

    export_public_data_json_fenix_derived__fenix_use_counters__v2 = GKEPodOperator(
        task_id="export_public_data_json_fenix_derived__fenix_use_counters__v2",
        name="export_public_data_json_fenix_derived__fenix_use_counters__v2",
        arguments=["script/publish_public_data_json"]
        + [
            "--query_file=sql/moz-fx-data-shared-prod/fenix_derived/fenix_use_counters_v2/query.sql"
        ]
        + ["--destination_table=fenix_use_counters${{ds_nodash}}"]
        + ["--dataset_id=fenix_derived"]
        + ["--project_id=moz-fx-data-shared-prod"]
        + ["--parameter=submission_date:DATE:{{ds}}"],
        image=docker_image,
    )

    export_public_data_json_firefox_desktop_derived__firefox_desktop_use_counters__v2 = GKEPodOperator(
        task_id="export_public_data_json_firefox_desktop_derived__firefox_desktop_use_counters__v2",
        name="export_public_data_json_firefox_desktop_derived__firefox_desktop_use_counters__v2",
        arguments=["script/publish_public_data_json"]
        + [
            "--query_file=sql/moz-fx-data-shared-prod/firefox_desktop_derived/firefox_desktop_use_counters_v2/query.sql"
        ]
        + ["--destination_table=firefox_desktop_use_counters${{ds_nodash}}"]
        + ["--dataset_id=firefox_desktop_derived"]
        + ["--project_id=moz-fx-data-shared-prod"]
        + ["--parameter=submission_date:DATE:{{ds}}"],
        image=docker_image,
    )

    export_public_data_json_glam_derived__client_probe_counts_firefox_desktop_beta__v1 = GKEPodOperator(
        task_id="export_public_data_json_glam_derived__client_probe_counts_firefox_desktop_beta__v1",
        name="export_public_data_json_glam_derived__client_probe_counts_firefox_desktop_beta__v1",
        arguments=["script/publish_public_data_json"]
        + [
            "--query_file=sql/moz-fx-data-shared-prod/glam_derived/client_probe_counts_firefox_desktop_beta_v1/query.sql"
        ]
        + ["--destination_table=client_probe_counts_firefox_desktop_beta"]
        + ["--dataset_id=glam_derived"]
        + ["--project_id=moz-fx-data-shared-prod"],
        image=docker_image,
    )

    export_public_data_json_glam_derived__client_probe_counts_firefox_desktop_nightly__v1 = GKEPodOperator(
        task_id="export_public_data_json_glam_derived__client_probe_counts_firefox_desktop_nightly__v1",
        name="export_public_data_json_glam_derived__client_probe_counts_firefox_desktop_nightly__v1",
        arguments=["script/publish_public_data_json"]
        + [
            "--query_file=sql/moz-fx-data-shared-prod/glam_derived/client_probe_counts_firefox_desktop_nightly_v1/query.sql"
        ]
        + ["--destination_table=client_probe_counts_firefox_desktop_nightly"]
        + ["--dataset_id=glam_derived"]
        + ["--project_id=moz-fx-data-shared-prod"],
        image=docker_image,
    )

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
    )

    wait_for_copy_deduplicate_all = ExternalTaskSensor(
        task_id="wait_for_copy_deduplicate_all",
        external_dag_id="copy_deduplicate",
        external_task_id="copy_deduplicate_all",
        execution_delta=datetime.timedelta(seconds=14400),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    export_public_data_json_fenix_derived__fenix_use_counters__v2.set_upstream(
        wait_for_copy_deduplicate_all
    )

    export_public_data_json_firefox_desktop_derived__firefox_desktop_use_counters__v2.set_upstream(
        wait_for_copy_deduplicate_all
    )

    export_public_data_json_mozregression_aggregates__v1.set_upstream(
        wait_for_copy_deduplicate_all
    )

    wait_for_copy_deduplicate_main_ping = ExternalTaskSensor(
        task_id="wait_for_copy_deduplicate_main_ping",
        external_dag_id="copy_deduplicate",
        external_task_id="copy_deduplicate_main_ping",
        execution_delta=datetime.timedelta(seconds=14400),
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    export_public_data_json_telemetry_derived__ssl_ratios__v1.set_upstream(
        wait_for_copy_deduplicate_main_ping
    )

    public_data_gcs_metadata = gke_command(
        task_id="public_data_gcs_metadata",
        command=["script/publish_public_data_gcs_metadata"],
        docker_image=docker_image,
    )

    public_data_gcs_metadata.set_upstream(
        [
            export_public_data_json_client_probe_processes__v1,
            export_public_data_json_fenix_derived__fenix_use_counters__v2,
            export_public_data_json_firefox_desktop_derived__firefox_desktop_use_counters__v2,
            export_public_data_json_glam_derived__client_probe_counts_firefox_desktop_beta__v1,
            export_public_data_json_glam_derived__client_probe_counts_firefox_desktop_nightly__v1,
            export_public_data_json_mozregression_aggregates__v1,
            export_public_data_json_telemetry_derived__ssl_ratios__v1,
        ]
    )
