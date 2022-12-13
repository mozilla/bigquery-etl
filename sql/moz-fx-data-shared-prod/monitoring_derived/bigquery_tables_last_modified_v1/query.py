"""Get BigQuery tables last modified date."""

from argparse import ArgumentParser

from google.cloud import bigquery

DEFAULT_PROJECTS = [
    "mozdata",
    "moz-fx-data-shared-prod",
    "moz-fx-data-marketing-prod",
]

parser = ArgumentParser(description=__doc__)
parser.add_argument("--project", default="moz-fx-data-shared-prod")
# projects queries were run from that access table
parser.add_argument("--source_projects", nargs="+", default=DEFAULT_PROJECTS)
parser.add_argument("--destination_dataset", default="monitoring_derived")
parser.add_argument("--destination_table", default="bigquery_tables_last_modified_v1")


def main():
    """Run query for each source project."""
    args = parser.parse_args()

    destination_table = (
        f"{args.project}.{args.destination_dataset}.{args.destination_table}"
    )

    # remove old table in case of re-run
    client = bigquery.Client(args.project)
    client.delete_table(destination_table, not_found_ok=True)

    for project in args.source_projects:
        client = bigquery.Client(project)

        datasets = list(client.list_datasets())

        for dataset in datasets:

            query = f"""
                  SELECT
                    project_id,
                    dataset_id,
                    table_id,
                    DATE(TIMESTAMP_MILLIS(creation_time)) AS creation_date,
                    DATE(TIMESTAMP_MILLIS(last_modified_time)) AS last_modified_date
                    FROM `{project}.{dataset.dataset_id}.__TABLES__`
            """

            try:
                job_config = bigquery.QueryJobConfig(
                    destination=destination_table, write_disposition="WRITE_APPEND"
                )
                client.query(query, job_config=job_config).result()

                client.query(query).result()

            except Exception as e:
                print(f"Error querying dataset {dataset.dataset_id}: {e}")


if __name__ == "__main__":
    main()
