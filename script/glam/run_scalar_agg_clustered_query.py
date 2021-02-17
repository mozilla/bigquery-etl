"""Run a query on clients_scalar_aggregates on one app_version at a time."""
import datetime
import sys
import time
from pathlib import Path

import click
from google.cloud import bigquery

VERSION_QUERY_TEMPLATE = """
SELECT
  DISTINCT(app_version)
FROM
  `{project}.{dataset}.clients_scalar_aggregates_v1`
WHERE
  submission_date = '{date}'
"""

SQL_BASE_DIR = (
    Path(__file__).parent.parent.parent
    / "sql"
    / "moz-fx-data-shared-prod"
    / "telemetry_derived"
)


@click.command()
@click.option("--submission-date", required=True, type=datetime.date.fromisoformat)
@click.option("--dst-table", required=True)
@click.option(
    "--project", default="moz-fx-data-shared-prod", help="Project for final table"
)
@click.option("--tmp-project", help="Project for temporary intermediate tables")
@click.option("--dataset", default="telemetry_derived")
def main(submission_date, dst_table, project, tmp_project, dataset):
    """Run query per app_version."""
    bq_client = bigquery.Client(project=project)

    app_versions = [
        row["app_version"]
        for row in bq_client.query(
            VERSION_QUERY_TEMPLATE.format(
                date=submission_date, project=project, dataset=dataset
            )
        ).result()
    ]

    print(f"Found versions: {app_versions}")

    if len(app_versions) == 0:
        print("Source table empty", file=sys.stderr)
        sys.exit(1)

    sql_path = SQL_BASE_DIR / dst_table / "query.sql"

    query_text = sql_path.read_text()

    # Write to intermediate table to avoid partial writes to destination table
    if tmp_project is None:
        tmp_project = project
    intermediate_table = f"{tmp_project}.analysis.glam_temp_clustered_query_{dst_table}"
    print(f"Writing results to {intermediate_table}")

    for i, app_version in enumerate(app_versions):
        print(f"Querying for app_version {app_version}")

        query_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter(
                    "submission_date", "DATE", str(submission_date)
                ),
                bigquery.ScalarQueryParameter("app_version", "INT64", app_version),
            ],
            clustering_fields=["metric", "channel"],
            destination=intermediate_table,
            default_dataset=f"{project}.{dataset}",
            write_disposition=(
                bigquery.WriteDisposition.WRITE_TRUNCATE
                if i == 0
                else bigquery.WriteDisposition.WRITE_APPEND
            ),
        )

        query_job = bq_client.query(query_text, job_config=query_config)

        # Periodically print so airflow gke operator doesn't think task is dead
        elapsed = 0
        while not query_job.done():
            time.sleep(10)
            elapsed += 10
            if elapsed % 200 == 10:
                print("Waiting on query...")

        print(f"Total elapsed: approximately {elapsed} seconds")

        results = query_job.result()
        print(f"Query job {query_job.job_id} finished")
        print(f"{results.total_rows} rows in {intermediate_table}")

    copy_config = bigquery.CopyJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    print(f"Copying {intermediate_table} to {project}.{dataset}.{dst_table}")
    bq_client.copy_table(
        intermediate_table,
        f"{project}.{dataset}.{dst_table}",
        job_config=copy_config,
    ).result()

    print(f"Deleting {intermediate_table}")
    bq_client.delete_table(intermediate_table)


if __name__ == "__main__":
    main()
