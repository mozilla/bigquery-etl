# Generated via https://github.com/mozilla/bigquery-etl/blob/main/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskMarker
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
import datetime
from utils.constants import ALLOWED_STATES, FAILED_STATES
from utils.gcp import bigquery_etl_query, gke_command, bigquery_dq_check

docs = """
### bqetl_mdn_yari

Built from bigquery-etl repo, [`dags/bqetl_mdn_yari.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_mdn_yari.py)

#### Description

Monthly data exports of MDN 'Popularities'. This aggregates and counts total
page visits and normalizes them agains the max.

#### Owner

fmerz@mozilla.com

#### Tags

* impact/tier_3
* repo/bigquery-etl
* triage/record_only
"""


default_args = {
    "owner": "fmerz@mozilla.com",
    "start_date": datetime.datetime(2023, 2, 1, 0, 0),
    "end_date": None,
    "email": [
        "telemetry-alerts@mozilla.com",
        "mdn-infra@mozilla.com",
        "fmerz@mozilla.com",
        "kik@mozilla.com",
    ],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=300),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
}

tags = ["impact/tier_3", "repo/bigquery-etl", "triage/record_only"]

with DAG(
    "bqetl_mdn_yari",
    default_args=default_args,
    schedule_interval="0 0 1 * *",
    doc_md=docs,
    tags=tags,
) as dag:
    mdn_yari_derived__mdn_popularities__v1 = gke_command(
        task_id="mdn_yari_derived__mdn_popularities__v1",
        command=[
            "python",
            "sql/moz-fx-data-shared-prod/mdn_yari_derived/mdn_popularities_v1/query.py",
        ]
        + ["--date", "{{ ds }}"],
        docker_image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
        owner="fmerz@mozilla.com",
        email=[
            "fmerz@mozilla.com",
            "kik@mozilla.com",
            "mdn-infra@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
    )
