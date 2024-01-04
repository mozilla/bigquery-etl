# Generated via https://github.com/mozilla/bigquery-etl/blob/main/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskMarker
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
import datetime
from utils.constants import ALLOWED_STATES, FAILED_STATES
from utils.gcp import bigquery_etl_query, gke_command, bigquery_dq_check

docs = """
### bqetl_telemetry_dev_cycle

Built from bigquery-etl repo, [`dags/bqetl_telemetry_dev_cycle.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_telemetry_dev_cycle.py)

#### Description

DAG for Telemetry Dev Cycle Dashboard
#### Owner

leli@mozilla.com

#### Tags

* impact/tier_3
* repo/bigquery-etl
"""


default_args = {
    "owner": "leli@mozilla.com",
    "start_date": datetime.datetime(2023, 12, 19, 0, 0),
    "end_date": None,
    "email": ["telemetry-alerts@mozilla.com", "leli@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
}

tags = ["impact/tier_3", "repo/bigquery-etl"]

with DAG(
    "bqetl_telemetry_dev_cycle",
    default_args=default_args,
    schedule_interval="0 18 * * *",
    doc_md=docs,
    tags=tags,
) as dag:
    telemetry_dev_cycle_derived__firefox_major_release_dates__v1 = bigquery_etl_query(
        task_id="telemetry_dev_cycle_derived__firefox_major_release_dates__v1",
        destination_table="firefox_major_release_dates_v1",
        dataset_id="telemetry_dev_cycle_derived",
        project_id="moz-fx-data-shared-prod",
        owner="leli@mozilla.com",
        email=["leli@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    telemetry_dev_cycle_derived__glean_metrics_stats__v1 = bigquery_etl_query(
        task_id="telemetry_dev_cycle_derived__glean_metrics_stats__v1",
        destination_table="glean_metrics_stats_v1",
        dataset_id="telemetry_dev_cycle_derived",
        project_id="moz-fx-data-shared-prod",
        owner="leli@mozilla.com",
        email=["leli@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    telemetry_dev_cycle_derived__telemetry_probes_stats__v1 = bigquery_etl_query(
        task_id="telemetry_dev_cycle_derived__telemetry_probes_stats__v1",
        destination_table="telemetry_probes_stats_v1",
        dataset_id="telemetry_dev_cycle_derived",
        project_id="moz-fx-data-shared-prod",
        owner="leli@mozilla.com",
        email=["leli@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        task_concurrency=1,
    )

    telemetry_dev_cycle_external__data_review_stats__v1 = gke_command(
        task_id="telemetry_dev_cycle_external__data_review_stats__v1",
        command=[
            "python",
            "sql/moz-fx-data-shared-prod/telemetry_dev_cycle_external/data_review_stats_v1/query.py",
        ]
        + [],
        docker_image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
        owner="leli@mozilla.com",
        email=["leli@mozilla.com", "telemetry-alerts@mozilla.com"],
    )

    telemetry_dev_cycle_external__experiments_stats__v1 = gke_command(
        task_id="telemetry_dev_cycle_external__experiments_stats__v1",
        command=[
            "python",
            "sql/moz-fx-data-shared-prod/telemetry_dev_cycle_external/experiments_stats_v1/query.py",
        ]
        + [],
        docker_image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
        owner="leli@mozilla.com",
        email=["leli@mozilla.com", "telemetry-alerts@mozilla.com"],
    )

    telemetry_dev_cycle_external__glean_metrics_stats__v1 = gke_command(
        task_id="telemetry_dev_cycle_external__glean_metrics_stats__v1",
        command=[
            "python",
            "sql/moz-fx-data-shared-prod/telemetry_dev_cycle_external/glean_metrics_stats_v1/query.py",
        ]
        + [],
        docker_image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
        owner="leli@mozilla.com",
        email=["leli@mozilla.com", "telemetry-alerts@mozilla.com"],
    )

    telemetry_dev_cycle_external__telemetry_probes_stats__v1 = gke_command(
        task_id="telemetry_dev_cycle_external__telemetry_probes_stats__v1",
        command=[
            "python",
            "sql/moz-fx-data-shared-prod/telemetry_dev_cycle_external/telemetry_probes_stats_v1/query.py",
        ]
        + [],
        docker_image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
        owner="leli@mozilla.com",
        email=["leli@mozilla.com", "telemetry-alerts@mozilla.com"],
    )

    telemetry_dev_cycle_derived__glean_metrics_stats__v1.set_upstream(
        telemetry_dev_cycle_derived__firefox_major_release_dates__v1
    )

    telemetry_dev_cycle_derived__glean_metrics_stats__v1.set_upstream(
        telemetry_dev_cycle_external__glean_metrics_stats__v1
    )

    telemetry_dev_cycle_derived__telemetry_probes_stats__v1.set_upstream(
        telemetry_dev_cycle_derived__firefox_major_release_dates__v1
    )

    telemetry_dev_cycle_derived__telemetry_probes_stats__v1.set_upstream(
        telemetry_dev_cycle_external__telemetry_probes_stats__v1
    )
