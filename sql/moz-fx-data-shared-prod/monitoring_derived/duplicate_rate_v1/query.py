#!/usr/bin/env python3

"""Generates counts of duplicate document_id values per table per day."""
import datetime
import json
from argparse import ArgumentParser

from google.cloud import bigquery

NAMESPACE_QUERY = """
SELECT
  schema_name
FROM
  `moz-fx-data-shared-prod`.INFORMATION_SCHEMA.SCHEMATA
WHERE
  schema_name LIKE "%_live"
ORDER BY
  schema_name
"""

QUERY = """
-- duplicate rate {date} {namespace};
SELECT
  DATE "{date}" AS submission_date,
  "{namespace}" AS document_namespace,
  `moz-fx-data-shared-prod`.udf.extract_document_type(_TABLE_SUFFIX) AS document_type,
  `moz-fx-data-shared-prod`.udf.extract_document_version(_TABLE_SUFFIX) AS document_version,
  COUNT(*) AS n,
  COUNT(DISTINCT document_id) AS n_distinct,
FROM
  `moz-fx-data-shared-prod.{namespace}_live.*`
WHERE
  -- only observe full days of data
  DATE(submission_timestamp) = date "{date}"
GROUP BY
  submission_date,
  document_namespace,
  document_type,
  document_version
"""


def duplicate_rate(
    date, project, destination_dataset, destination_table, skip_exceptions
):
    """Get rate of duplicates in each live table for the given day."""
    client = bigquery.Client(project=project)
    print("Getting the list of namespaces...")
    namespaces = [
        row.schema_name.split("_live")[0]
        for row in client.query(NAMESPACE_QUERY).result()
    ]
    print(namespaces)

    mb_processed = {}
    for i, namespace in enumerate(namespaces):
        print(f"Running job for {namespace}")
        try:
            job = client.query(
                QUERY.format(date=date, namespace=namespace),
                job_config=bigquery.QueryJobConfig(
                    time_partitioning=bigquery.TimePartitioning(
                        field="submission_date"
                    ),
                    create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
                    write_disposition=(
                        # only the first query should overwrite the partition
                        bigquery.WriteDisposition.WRITE_TRUNCATE
                        if i == 0
                        else bigquery.WriteDisposition.WRITE_APPEND
                    ),
                    destination=(
                        f"{project}.{destination_dataset}.{destination_table}"
                        f"${date.strftime('%Y%m%d')}"
                    ),
                ),
            )
            job.result()
            mb_processed[namespace] = round(job.total_bytes_processed / 1024**2, 1)
        except Exception as e:
            if not skip_exceptions:
                raise
            print(e)
    # summary info
    print(json.dumps(mb_processed, indent=2))


def parse_args():
    """Parse command line arguments."""
    parser = ArgumentParser(description=__doc__)
    parser.add_argument("--date", type=datetime.date.fromisoformat, required=True)
    parser.add_argument("--project", default="moz-fx-data-shared-prod")
    parser.add_argument("--destination-dataset", default="monitoring_derived")
    parser.add_argument("--destination-table", default="duplicate_rate_v1")
    parser.add_argument(
        "--skip-exceptions",
        action="store_true",
        default=False,
        help="continue when running into errors during query time",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    duplicate_rate(
        args.date,
        args.project,
        args.destination_dataset,
        args.destination_table,
        args.skip_exceptions,
    )
