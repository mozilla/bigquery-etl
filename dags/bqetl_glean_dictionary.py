# Generated via https://github.com/mozilla/bigquery-etl/blob/main/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskMarker
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
import datetime
from operators.gcp_container_operator import GKEPodOperator
from utils.constants import ALLOWED_STATES, FAILED_STATES
from utils.gcp import bigquery_etl_query, bigquery_dq_check, bigquery_bigeye_check

docs = """
### bqetl_glean_dictionary

Built from bigquery-etl repo, [`dags/bqetl_glean_dictionary.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_glean_dictionary.py)

#### Description

Extracts Glean auto-event metadata used by Glean Dictionary
#### Owner

efilho@mozilla.com

#### Tags

* impact/tier_3
* repo/bigquery-etl
"""


default_args = {
    "owner": "efilho@mozilla.com",
    "start_date": datetime.datetime(2025, 2, 5, 0, 0),
    "end_date": None,
    "email": ["efilho@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
}

tags = ["impact/tier_3", "repo/bigquery-etl"]

with DAG(
    "bqetl_glean_dictionary",
    default_args=default_args,
    schedule_interval="0 2 * * *",
    doc_md=docs,
    tags=tags,
) as dag:

    wait_for_accounts_frontend_derived__events_stream__v1 = ExternalTaskSensor(
        task_id="wait_for_accounts_frontend_derived__events_stream__v1",
        external_dag_id="bqetl_glean_usage",
        external_task_id="accounts_frontend.accounts_frontend_derived__events_stream__v1",
        check_existence=True,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=5),
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    glean_auto_events_derived__apps_auto_events_metadata__v1 = bigquery_etl_query(
        task_id="glean_auto_events_derived__apps_auto_events_metadata__v1",
        destination_table="apps_auto_events_metadata_v1",
        dataset_id="glean_auto_events_derived",
        project_id="moz-fx-data-shared-prod",
        owner="efilho@mozilla.com",
        email=["efilho@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=False,
    )

    glean_auto_events_derived__daily_auto_events_metadata__v1 = bigquery_etl_query(
        task_id="glean_auto_events_derived__daily_auto_events_metadata__v1",
        destination_table="daily_auto_events_metadata_v1",
        dataset_id="glean_auto_events_derived",
        project_id="moz-fx-data-shared-prod",
        owner="efilho@mozilla.com",
        email=["efilho@mozilla.com"],
        date_partition_parameter="submission_date",
        depends_on_past=True,
    )

    with TaskGroup(
        "glean_auto_events_derived__daily_auto_events_metadata__v1_external",
    ) as glean_auto_events_derived__daily_auto_events_metadata__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_public_data_json__wait_for_glean_auto_events_derived__daily_auto_events_metadata__v1",
            external_dag_id="bqetl_public_data_json",
            external_task_id="wait_for_glean_auto_events_derived__daily_auto_events_metadata__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=75600)).isoformat() }}",
        )

        glean_auto_events_derived__daily_auto_events_metadata__v1_external.set_upstream(
            glean_auto_events_derived__daily_auto_events_metadata__v1
        )

    glean_auto_events_derived__apps_auto_events_metadata__v1.set_upstream(
        glean_auto_events_derived__daily_auto_events_metadata__v1
    )

    glean_auto_events_derived__daily_auto_events_metadata__v1.set_upstream(
        wait_for_accounts_frontend_derived__events_stream__v1
    )
