# Generated via https://github.com/mozilla/bigquery-etl/blob/main/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskMarker
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
import datetime
from utils.constants import ALLOWED_STATES, FAILED_STATES
from utils.gcp import bigquery_etl_query, gke_command, bigquery_dq_check

docs = """
### bqetl_download_funnel_attribution

Built from bigquery-etl repo, [`dags/bqetl_download_funnel_attribution.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_download_funnel_attribution.py)

#### Description

Daily aggregations of data exported from Google Analytics joined with Firefox download data.
#### Owner

gleonard@mozilla.com

#### Tags

* impact/tier_1
* repo/bigquery-etl
"""


default_args = {
    "owner": "gleonard@mozilla.com",
    "start_date": datetime.datetime(2023, 4, 10, 0, 0),
    "end_date": None,
    "email": ["gleonard@mozilla.com", "telemetry-alerts@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
}

tags = ["impact/tier_1", "repo/bigquery-etl"]

with DAG(
    "bqetl_download_funnel_attribution",
    default_args=default_args,
    schedule_interval="0 23 * * *",
    doc_md=docs,
    tags=tags,
) as dag:
    checks__fail_ga_derived__downloads_with_attribution__v2 = bigquery_dq_check(
        task_id="checks__fail_ga_derived__downloads_with_attribution__v2",
        source_table='downloads_with_attribution_v2${{ macros.ds_format(macros.ds_add(ds, -1), "%Y-%m-%d", "%Y%m%d") }}',
        dataset_id="ga_derived",
        project_id="moz-fx-data-marketing-prod",
        is_dq_check_fail=True,
        owner="gleonard@mozilla.com",
        email=["gleonard@mozilla.com", "telemetry-alerts@mozilla.com"],
        depends_on_past=False,
        parameters=["download_date:DATE:{{macros.ds_add(ds, -1)}}"],
        retries=0,
    )

    ga_derived__downloads_with_attribution__v2 = bigquery_etl_query(
        task_id="ga_derived__downloads_with_attribution__v2",
        destination_table='downloads_with_attribution_v2${{ macros.ds_format(macros.ds_add(ds, -1), "%Y-%m-%d", "%Y%m%d") }}',
        dataset_id="ga_derived",
        project_id="moz-fx-data-marketing-prod",
        owner="gleonard@mozilla.com",
        email=["gleonard@mozilla.com", "telemetry-alerts@mozilla.com"],
        date_partition_parameter=None,
        depends_on_past=False,
        parameters=["download_date:DATE:{{macros.ds_add(ds, -1)}}"],
    )

    checks__fail_ga_derived__downloads_with_attribution__v2.set_upstream(
        ga_derived__downloads_with_attribution__v2
    )

    wait_for_ga_derived__www_site_empty_check__v1 = ExternalTaskSensor(
        task_id="wait_for_ga_derived__www_site_empty_check__v1",
        external_dag_id="bqetl_google_analytics_derived",
        external_task_id="ga_derived__www_site_empty_check__v1",
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    ga_derived__downloads_with_attribution__v2.set_upstream(
        wait_for_ga_derived__www_site_empty_check__v1
    )
