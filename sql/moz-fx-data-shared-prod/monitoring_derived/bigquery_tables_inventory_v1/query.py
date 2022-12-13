"""
Get BigQuery tables inventory.

To read more on the source table, please visit:
https://cloud.google.com/bigquery/docs/information-schema-tables
"""

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
parser.add_argument("--destination_table", default="bigquery_tables_inventory_v1")
parser.add_argument("--tmp_table", default="bigquery_tables_last_modified_tmp")


def create_last_modified_tmp_table(project, tmp_table_name):
    """Create temp table to capture last modified dates."""
    # remove old table in case of re-run
    client = bigquery.Client(project)
    client.delete_table(tmp_table_name, not_found_ok=True)

    tmp_table = bigquery.Table(tmp_table_name)
    tmp_table.schema = (
        bigquery.SchemaField("project_id", "STRING"),
        bigquery.SchemaField("dataset_id", "STRING"),
        bigquery.SchemaField("table_id", "STRING"),
        bigquery.SchemaField("creation_date", "DATE"),
        bigquery.SchemaField("last_modified_date", "DATE"),
    )
    client.create_table(tmp_table)

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
                destination=tmp_table_name, write_disposition="WRITE_APPEND"
            )
            client.query(query, job_config=job_config).result()

        except Exception as e:
            print(f"Error querying dataset {dataset.dataset_id}: {e}")


def create_query(source_project, tmp_table_name):
    """Create query for a source project."""
    return f"""
        SELECT *
        FROM
            (SELECT
              DATE(creation_time) AS creation_date,
              table_catalog AS project_id,
              table_schema AS dataset_id,
              table_name AS table_id,
              table_type,
              FROM `{source_project}.region-us.INFORMATION_SCHEMA.TABLES`
              )
        LEFT JOIN {tmp_table_name}
        USING (project_id, dataset_id, table_id, creation_date)
        ORDER BY creation_date, project_id, dataset_id, table_id, table_type
    """


def main():
    """Run query for each source project."""
    args = parser.parse_args()

    tmp_table_name = f"{args.project}.{args.destination_dataset}.{args.tmp_table}"

    destination_table = (
        f"{args.project}.{args.destination_dataset}.{args.destination_table}"
    )

    # remove old table in case of re-run
    client = bigquery.Client(args.project)
    client.delete_table(destination_table, not_found_ok=True)

    for project in args.source_projects:

        create_last_modified_tmp_table(project, tmp_table_name)

        client = bigquery.Client(project)
        query = create_query(project, tmp_table_name)
        job_config = bigquery.QueryJobConfig(
            destination=destination_table, write_disposition="WRITE_APPEND"
        )
        client.query(query, job_config=job_config).result()

    client.delete_table(tmp_table_name, not_found_ok=True)


if __name__ == "__main__":
    main()
