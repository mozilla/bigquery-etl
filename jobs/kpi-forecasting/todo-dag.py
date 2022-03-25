"""
See [etl-graph in the docker-etl repository]
(https://github.com/mozilla/docker-etl/blob/main/jobs/etl-graph).
"""

from datetime import datetime, timedelta

from airflow import DAG
from operators.gcp_container_operator import GKEPodOperator
from utils.tags import Tag

default_args = {
    "owner": "pmcmanis@mozilla.com",
    "email": ["pmcmanis@mozilla.com"],
    "depends_on_past": False,
    "start_date": datetime(2021, 2, 1),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=30),
}

tags = [Tag.ImpactTier.tier_3] # TODO: figure this out

with DAG("etl_graph", default_args=default_args, schedule_interval="0 2 * * sat", doc_md=__doc__, tags=tags,) as dag:
    dataset_yamls = ["yaml/mobile.yaml", "yaml/desktop.yaml"]

    experiment_enrollment_export = gke_command(
        task_id="experiment_enrollment_export",
        command=[
                    "python", "kpi-forecasting/kpi-forecasting.py",
                    "-c",
                ] + dataset_yamls,
        docker_image="gcr.io/moz-fx-data-airflow-prod-88e0/kpi-forecasting_docker_etl:latest",
        dag=dag,
    )