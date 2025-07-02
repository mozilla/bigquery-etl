from datetime import datetime, timedelta
from pathlib import Path

import click
from google.cloud import bigquery

from bigquery_etl.util.common import TempDatasetReference

query = (Path(__file__).parent / "query_supplemental.sql").read_text()


@click.command()
@click.option(
    "--submission-date",
    required=True,
    type=click.DateTime(formats=["%Y-%m-%d"]),
)
@click.option(
    "--billing-project",
    default=None,
    help="Project to use to run queries",
)
@click.option(
    "--destination-table",
    required=True,
    type=bigquery.TableReference.from_string,
    help="Table to write final results to, formatted as PROJECT_ID.DATASET_ID.TABLE_ID",
)
@click.option(
    "--temp-dataset",
    default="moz-fx-data-shared-prod.tmp",
    type=TempDatasetReference.from_string,
    help="Dataset where intermediate query results will be temporarily stored, "
    "formatted as PROJECT_ID.DATASET_ID",
)
@click.option(
    "--slices",
    type=int,
    default=1,
    help="Number of queries to split events stream query into, based on sample id",
)
def main(submission_date, billing_project, destination_table, temp_dataset, slices):
    client = bigquery.Client(project=billing_project)

    sample_id_interval_size = 100 // slices

    destination_table = client.get_table(destination_table)

    temp_tables = []

    for min_sample_id in range(0, 100, sample_id_interval_size):
        max_sample_id = min_sample_id + (sample_id_interval_size - 1)

        # create temp table
        temp_table = bigquery.Table(temp_dataset.temp_table())
        temp_table.schema = destination_table.schema
        temp_table.time_partitioning = destination_table.time_partitioning
        temp_table.clustering_fields = destination_table.clustering_fields
        temp_table.expires = datetime.utcnow() + timedelta(days=1)
        temp_table = client.create_table(temp_table)

        job = client.query(
            query=query,
            job_config=bigquery.QueryJobConfig(
                destination=temp_table,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                query_parameters=[
                    bigquery.ScalarQueryParameter(
                        "min_sample_id", "INT64", min_sample_id
                    ),
                    bigquery.ScalarQueryParameter(
                        "max_sample_id", "INT64", max_sample_id
                    ),
                    bigquery.ScalarQueryParameter(
                        "submission_date", "DATE", submission_date.date()
                    ),
                ],
            ),
        )
        print(
            f"Writing sample id {min_sample_id} to {max_sample_id} to {temp_table}: {job.path}"
        )
        job.result()
        temp_tables.append(job.destination)

    print(f"Copying to {destination_table}")
    client.copy_table(
        sources=temp_tables,
        destination=f"{destination_table}${submission_date.strftime('%Y%m%d')}",
        job_config=bigquery.CopyJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
        ),
    )


if __name__ == "__main__":
    main()
