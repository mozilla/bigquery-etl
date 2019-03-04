import datetime

from airflow import models
from airflow.contrib.operators.bigquery_operator import BigQueryOperator

with models.DAG(
    "clients_daily",
    schedule_interval="0 1 * * *",
    default_args={
        "start_date": datetime.datetime(2019, 3, 1),
        "email": ["dthorn@mozilla.com", "dataops+alerts@mozilla.com"],
        "email_on_failure": True,
        "email_on_retry": False,
        "depends_on_past": True,
        # If a task fails, retry it once after waiting at least 5 minutes
        "retries": 0,
        "retry_delay": datetime.timedelta(minutes=5),
        "project_id": models.Variable.get("gcp_project"),
    },
) as dag:
    clients_daily = BigQueryOperator(
        task_id="clients_daily",
        bql="sql/clients_daily_v7.sql",
        destination_dataset_table="analysis.clients_daily_v7${{ds_nodash}}",
        write_disposition="WRITE_TRUNCATE",
        use_legacy_sql=False,
    )

    clients_last_seen = BigQueryOperator(
        task_id="clients_last_seen",
        bql="sql/clients_last_seen_v1.sql",
        destination_dataset_table="analysis.clients_last_seen_v1${{ds_nodash}}",
        write_disposition="WRITE_TRUNCATE",
        use_legacy_sql=False,
    )

    clients_daily >> clients_last_seen

    exact_mau_by_dimensions = BigQueryOperator(
        task_id="exact_mau_by_dimensions",
        bql="sql/firefox_desktop_exact_mau28_by_dimensions_v1.sql",
        destination_dataset_table="analysis.firefox_desktop_exact_mau28_by_dimensions_v1${{ds_nodash}}",
        write_disposition="WRITE_TRUNCATE",
        use_legacy_sql=False,
    )

    clients_last_seen >> exact_mau_by_dimensions

    exact_mau = BigQueryOperator(
        task_id="exact_mau",
        bql="sql/firefox_desktop_exact_mau28_v1.sql",
        destination_dataset_table="analysis.firefox_desktop_exact_mau28_v1${{ds_nodash}}",
        write_disposition="WRITE_TRUNCATE",
        use_legacy_sql=False,
    )

    exact_mau_by_dimensions >> exact_mau
