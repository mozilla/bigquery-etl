"""
Runs SQL queries and writes results to destination tables.

When executing a query associated metadata is parsed to determine whether
results should be written to a corresponding public dataset.
"""

import logging
import re
import sys
from argparse import ArgumentParser
from pathlib import Path

import yaml
from google.cloud import bigquery

from bigquery_etl.metadata.parse_metadata import Metadata
from bigquery_etl.metadata.validate_metadata import validate_public_data

DESTINATION_TABLE_RE = re.compile(r"^[a-zA-Z0-9_$]{0,1024}$")
PUBLIC_PROJECT_ID = "mozilla-public-data"


parser = ArgumentParser(description=__doc__)
parser.add_argument(
    "--public_project_id",
    default=PUBLIC_PROJECT_ID,
    help="Project with publicly accessible data",
)
parser.add_argument(
    "--destination_table", help="Destination table name results are written to"
)
parser.add_argument(
    "--project_id",
    help="Default project, if not specified the sdk will determine one",
)
parser.add_argument(
    "--time_partitioning_field",
    type=lambda f: bigquery.TimePartitioning(field=f),
    help="time partition field on the destination table",
)
parser.add_argument(
    "--clustering_fields",
    type=lambda f: f.split(","),
    help="comma separated list of clustering fields on the destination table",
)
parser.add_argument(
    "--dry_run",
    action="store_true",
    help="Print bytes that would be processed for each part and don't run queries",
)
parser.add_argument(
    "--parameter",
    action="append",
    default=[],
    dest="parameters",
    type=lambda p: bigquery.ScalarQueryParameter(*p.split(":", 2)),
    metavar="NAME:TYPE:VALUE",
    help="query parameter(s) to pass when running parts",
)
parser.add_argument(
    "--max_rows",
    help="The number of rows to return in the query results.",
    default=0,
)
parser.add_argument(
    "--schema_update_option",
    action="append",
    choices=[
        bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
        bigquery.SchemaUpdateOption.ALLOW_FIELD_RELAXATION,
        # Airflow passes an empty string when the field addition date doesn't
        # match the run date.
        # See https://github.com/mozilla/telemetry-airflow/blob/
        # e49fa7e6b3f5ec562dd248d257770c2303cf0cba/dags/utils/gcp.py#L515
        "",
    ],
    default=[],
    dest="schema_update_options",
    help="Optional options for updating the schema.",
)
parser.add_argument(
    "--replace",
    action="store_true",
    help="Whether to overwrite the `destination_table` or not.",
)
parser.add_argument(
    "--write_disposition",
    default=bigquery.WriteDisposition.WRITE_APPEND,
    help="The action that occurs if destination table already exists.",
)
parser.add_argument("--dataset_id", help="Destination dataset")
parser.add_argument("--query_file", help="File path to query to be executed")


def run(
    query_file,
    dataset_id=None,
    destination_table=None,
    project_id=None,
    public_project_id=PUBLIC_PROJECT_ID,
    dry_run=False,
    parameters=None,
    replace=False,
    schema_update_options=None,
    time_partitioning_field=None,
    clustering_fields=None,
    max_rows=0,
    write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
):
    """Run the specified query with the provided arguments."""
    schema_update_options = schema_update_options or []
    parameters = parameters or []
    clustering_fields = clustering_fields or []

    if schema_update_options is None:
        schema_update_options = []
    query_path = Path(query_file)
    if not query_path.exists():
        raise FileNotFoundError(f"Path to query: {query_path} does not exist.")

    use_public_table = False

    try:
        metadata = Metadata.of_query_file(query_file)
        if metadata.is_public_bigquery():
            if not validate_public_data(metadata, query_file):
                sys.exit(1)

            use_public_table = True
    except yaml.YAMLError as e:
        logging.error(e)
        sys.exit(1)
    except FileNotFoundError:
        logging.warning(f"INFO: No metadata.yaml found for {query_file}")

    can_qualify_destination_table = (
        dataset_id is not None
        and destination_table is not None
        and re.match(DESTINATION_TABLE_RE, destination_table)
    )

    if not can_qualify_destination_table:
        raise ValueError(
            "ERROR: Unable to qualify destination table."
            " --destination_table=<table without dataset ID> and"
            " --dataset_id=<dataset> required"
        )

    if use_public_table:
        # change the destination table to write results to the public dataset;
        # a view to the public table in the internal dataset is created
        # when CI runs
        destination_project = public_project_id
    else:
        destination_project = project_id

    if destination_project is None:
        logging.info(
            "No project provided, the BigQuery Client will determine the project "
            "(from environment variables or a .bigqueryrc file."
        )

    qualified_table = "{}.{}.{}".format(
        destination_project, dataset_id, destination_table
    )

    client = bigquery.Client(destination_project)

    write_disposition = (
        bigquery.WriteDisposition.WRITE_TRUNCATE if replace else write_disposition
    )

    job_config = bigquery.QueryJobConfig(
        destination=qualified_table,
        time_partitioning=time_partitioning_field,
        clustering_fields=clustering_fields,
        dry_run=dry_run,
        schema_update_options=schema_update_options,
        query_parameters=parameters,
        use_legacy_sql=False,
        write_disposition=write_disposition,
    )

    job = client.query(query=query_path.read_text(), job_config=job_config)

    if job.dry_run:
        logging.info(f"Would process {int(job.total_bytes_processed):,d} bytes")
    else:
        logging.info(f"Job with ID: {job.job_id} started at {job.started}")
        logging.info(f"Job URL: {job.self_link}")
        job.result(max_results=max_rows)
        logging.info(
            f"Job ended at {job.ended}; processed {int(job.total_bytes_processed):,d} bytes"
        )


def main():
    """Run query."""
    args = parser.parse_args()

    run(
        args.query_file,
        dataset_id=args.dataset_id,
        destination_table=args.destination_table,
        project_id=args.project_id,
        public_project_id=args.public_project_id,
        dry_run=args.dry_run,
        parameters=args.parameters,
        replace=args.replace,
        schema_update_options=args.schema_update_options,
        time_partitioning_field=args.time_partitioning_field,
        clustering_fields=args.clustering_fields,
        max_rows=args.max_rows,
        write_disposition=args.write_disposition,
    )


if __name__ == "__main__":
    main()
