# Generated via https://github.com/mozilla/bigquery-etl/blob/master/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from airflow.operators.sensors import ExternalTaskSensor
import datetime
from utils.gcp import bigquery_etl_query, gke_command

docs = """
### bqetl_main_summary

Built from bigquery-etl repo, [`dags/bqetl_main_summary.py`](https://github.com/mozilla/bigquery-etl/blob/master/dags/bqetl_main_summary.py)

#### Description

General-purpose derived tables for analyzing usage of desktop Firefox. This is one of our highest-impact DAGs and should be handled carefully.
#### Owner

dthorn@mozilla.com
"""


default_args = {
    "owner": "dthorn@mozilla.com",
    "start_date": datetime.datetime(2018, 11, 27, 0, 0),
    "end_date": None,
    "email": [
        "telemetry-alerts@mozilla.com",
        "dthorn@mozilla.com",
        "jklukas@mozilla.com",
        "frank@mozilla.com",
    ],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
}

with DAG(
    "bqetl_main_summary",
    default_args=default_args,
    schedule_interval="0 2 * * *",
    doc_md=docs,
) as dag:

    firefox_desktop_exact_mau28_by_client_count_dimensions = bigquery_etl_query(
        task_id="firefox_desktop_exact_mau28_by_client_count_dimensions",
        destination_table="firefox_desktop_exact_mau28_by_client_count_dimensions_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jklukas@mozilla.com",
        email=[
            "dthorn@mozilla.com",
            "frank@mozilla.com",
            "jklukas@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        dag=dag,
    )

    firefox_desktop_exact_mau28_by_dimensions = bigquery_etl_query(
        task_id="firefox_desktop_exact_mau28_by_dimensions",
        destination_table="firefox_desktop_exact_mau28_by_dimensions_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="dthorn@mozilla.com",
        email=[
            "dthorn@mozilla.com",
            "frank@mozilla.com",
            "jklukas@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        dag=dag,
    )

    firefox_desktop_exact_mau28_by_dimensions_v2 = bigquery_etl_query(
        task_id="firefox_desktop_exact_mau28_by_dimensions_v2",
        destination_table="firefox_desktop_exact_mau28_by_dimensions_v2",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jklukas@mozilla.com",
        email=[
            "dthorn@mozilla.com",
            "frank@mozilla.com",
            "jklukas@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter="submission_date",
        depends_on_past=False,
        dag=dag,
    )

    telemetry_derived__clients_daily__v6 = bigquery_etl_query(
        task_id="telemetry_derived__clients_daily__v6",
        destination_table="clients_daily_v6",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="dthorn@mozilla.com",
        email=[
            "dthorn@mozilla.com",
            "frank@mozilla.com",
            "jklukas@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        start_date=datetime.datetime(2019, 11, 5, 0, 0),
        date_partition_parameter="submission_date",
        depends_on_past=False,
        allow_field_addition_on_date="2020-11-13",
        dag=dag,
    )

    telemetry_derived__clients_daily_event__v1 = bigquery_etl_query(
        task_id="telemetry_derived__clients_daily_event__v1",
        destination_table="clients_daily_event_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jklukas@mozilla.com",
        email=[
            "dthorn@mozilla.com",
            "frank@mozilla.com",
            "jklukas@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        start_date=datetime.datetime(2021, 1, 19, 0, 0),
        date_partition_parameter="submission_date",
        depends_on_past=False,
        priority_weight=85,
        dag=dag,
    )

    telemetry_derived__clients_daily_joined__v1 = bigquery_etl_query(
        task_id="telemetry_derived__clients_daily_joined__v1",
        destination_table="clients_daily_joined_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jklukas@mozilla.com",
        email=[
            "dthorn@mozilla.com",
            "frank@mozilla.com",
            "jklukas@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        start_date=datetime.datetime(2021, 1, 19, 0, 0),
        date_partition_parameter="submission_date",
        depends_on_past=False,
        priority_weight=85,
        dag=dag,
    )

    telemetry_derived__clients_first_seen__v1 = bigquery_etl_query(
        task_id="telemetry_derived__clients_first_seen__v1",
        destination_table="clients_first_seen_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jklukas@mozilla.com",
        email=[
            "dthorn@mozilla.com",
            "frank@mozilla.com",
            "jklukas@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        start_date=datetime.datetime(2020, 5, 5, 0, 0),
        date_partition_parameter=None,
        depends_on_past=True,
        parameters=["submission_date:DATE:{{ds}}"],
        priority_weight=80,
        allow_field_addition_on_date="2020-10-20",
        dag=dag,
    )

    telemetry_derived__clients_last_seen__v1 = bigquery_etl_query(
        task_id="telemetry_derived__clients_last_seen__v1",
        destination_table="clients_last_seen_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="dthorn@mozilla.com",
        email=["dthorn@mozilla.com", "jklukas@mozilla.com"],
        start_date=datetime.datetime(2019, 4, 15, 0, 0),
        date_partition_parameter="submission_date",
        depends_on_past=True,
        priority_weight=85,
        allow_field_addition_on_date="2020-10-20",
        dag=dag,
    )

    telemetry_derived__clients_last_seen_event__v1 = bigquery_etl_query(
        task_id="telemetry_derived__clients_last_seen_event__v1",
        destination_table="clients_last_seen_event_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jklukas@mozilla.com",
        email=[
            "dthorn@mozilla.com",
            "frank@mozilla.com",
            "jklukas@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        start_date=datetime.datetime(2021, 1, 19, 0, 0),
        date_partition_parameter="submission_date",
        depends_on_past=True,
        priority_weight=85,
        dag=dag,
    )

    telemetry_derived__clients_last_seen_joined__v1 = bigquery_etl_query(
        task_id="telemetry_derived__clients_last_seen_joined__v1",
        destination_table="clients_last_seen_joined_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jklukas@mozilla.com",
        email=[
            "dthorn@mozilla.com",
            "frank@mozilla.com",
            "jklukas@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        start_date=datetime.datetime(2021, 1, 19, 0, 0),
        date_partition_parameter="submission_date",
        depends_on_past=True,
        priority_weight=85,
        dag=dag,
    )

    telemetry_derived__firefox_desktop_usage__v1 = bigquery_etl_query(
        task_id="telemetry_derived__firefox_desktop_usage__v1",
        destination_table="firefox_desktop_usage_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jklukas@mozilla.com",
        email=[
            "dthorn@mozilla.com",
            "frank@mozilla.com",
            "jklukas@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        date_partition_parameter=None,
        depends_on_past=False,
        dag=dag,
    )

    telemetry_derived__main_1pct__v1 = bigquery_etl_query(
        task_id="telemetry_derived__main_1pct__v1",
        destination_table="main_1pct_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jklukas@mozilla.com",
        email=[
            "dthorn@mozilla.com",
            "frank@mozilla.com",
            "jklukas@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        start_date=datetime.datetime(2020, 6, 1, 0, 0),
        date_partition_parameter="submission_date",
        depends_on_past=False,
        arguments=["--schema_update_option=ALLOW_FIELD_ADDITION"],
        dag=dag,
    )

    telemetry_derived__main_nightly__v1 = bigquery_etl_query(
        task_id="telemetry_derived__main_nightly__v1",
        destination_table="main_nightly_v1",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="jklukas@mozilla.com",
        email=[
            "dthorn@mozilla.com",
            "frank@mozilla.com",
            "jklukas@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        start_date=datetime.datetime(2020, 7, 1, 0, 0),
        date_partition_parameter="submission_date",
        depends_on_past=False,
        arguments=["--schema_update_option=ALLOW_FIELD_ADDITION"],
        dag=dag,
    )

    telemetry_derived__main_summary__v4 = bigquery_etl_query(
        task_id="telemetry_derived__main_summary__v4",
        destination_table="main_summary_v4",
        dataset_id="telemetry_derived",
        project_id="moz-fx-data-shared-prod",
        owner="dthorn@mozilla.com",
        email=[
            "dthorn@mozilla.com",
            "frank@mozilla.com",
            "jklukas@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        start_date=datetime.datetime(2019, 10, 25, 0, 0),
        date_partition_parameter="submission_date",
        depends_on_past=False,
        multipart=True,
        sql_file_path="sql/moz-fx-data-shared-prod/telemetry_derived/main_summary_v4",
        priority_weight=90,
        allow_field_addition_on_date="2020-11-02",
        dag=dag,
    )

    firefox_desktop_exact_mau28_by_client_count_dimensions.set_upstream(
        telemetry_derived__clients_last_seen__v1
    )

    firefox_desktop_exact_mau28_by_dimensions.set_upstream(
        telemetry_derived__clients_last_seen__v1
    )

    firefox_desktop_exact_mau28_by_dimensions_v2.set_upstream(
        telemetry_derived__clients_last_seen__v1
    )

    wait_for_copy_deduplicate_main_ping = ExternalTaskSensor(
        task_id="wait_for_copy_deduplicate_main_ping",
        external_dag_id="copy_deduplicate",
        external_task_id="copy_deduplicate_main_ping",
        execution_delta=datetime.timedelta(seconds=3600),
        check_existence=True,
        mode="reschedule",
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    telemetry_derived__clients_daily__v6.set_upstream(
        wait_for_copy_deduplicate_main_ping
    )

    wait_for_bq_main_events = ExternalTaskSensor(
        task_id="wait_for_bq_main_events",
        external_dag_id="copy_deduplicate",
        external_task_id="bq_main_events",
        execution_delta=datetime.timedelta(seconds=3600),
        check_existence=True,
        mode="reschedule",
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    telemetry_derived__clients_daily_event__v1.set_upstream(wait_for_bq_main_events)
    wait_for_event_events = ExternalTaskSensor(
        task_id="wait_for_event_events",
        external_dag_id="copy_deduplicate",
        external_task_id="event_events",
        execution_delta=datetime.timedelta(seconds=3600),
        check_existence=True,
        mode="reschedule",
        pool="DATA_ENG_EXTERNALTASKSENSOR",
    )

    telemetry_derived__clients_daily_event__v1.set_upstream(wait_for_event_events)

    telemetry_derived__clients_daily_joined__v1.set_upstream(
        telemetry_derived__clients_daily__v6
    )

    telemetry_derived__clients_daily_joined__v1.set_upstream(
        telemetry_derived__clients_daily_event__v1
    )

    telemetry_derived__clients_first_seen__v1.set_upstream(
        telemetry_derived__clients_daily__v6
    )

    telemetry_derived__clients_last_seen__v1.set_upstream(
        telemetry_derived__clients_daily__v6
    )

    telemetry_derived__clients_last_seen__v1.set_upstream(
        telemetry_derived__clients_first_seen__v1
    )

    telemetry_derived__clients_last_seen_event__v1.set_upstream(
        telemetry_derived__clients_daily_event__v1
    )

    telemetry_derived__clients_last_seen_joined__v1.set_upstream(
        telemetry_derived__clients_last_seen__v1
    )

    telemetry_derived__clients_last_seen_joined__v1.set_upstream(
        telemetry_derived__clients_last_seen_event__v1
    )

    telemetry_derived__firefox_desktop_usage__v1.set_upstream(
        firefox_desktop_exact_mau28_by_dimensions_v2
    )

    telemetry_derived__main_1pct__v1.set_upstream(wait_for_copy_deduplicate_main_ping)

    telemetry_derived__main_nightly__v1.set_upstream(
        wait_for_copy_deduplicate_main_ping
    )

    telemetry_derived__main_summary__v4.set_upstream(
        wait_for_copy_deduplicate_main_ping
    )
