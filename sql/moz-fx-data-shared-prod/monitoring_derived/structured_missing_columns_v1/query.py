#!/usr/bin/env python3

"""Generates the list of missing columns in structured datasets."""
import datetime
import json
from argparse import ArgumentParser
from collections import defaultdict

from google.cloud import bigquery

NAMESPACE_QUERY = """
SELECT
  schema_name
FROM
  `moz-fx-data-shared-prod`.INFORMATION_SCHEMA.SCHEMATA
WHERE
  schema_name LIKE "%_stable"
  AND schema_name NOT LIKE "%telemetry%"
ORDER BY
  schema_name
"""

QUERY = """
-- missing columns {date} {namespace};
WITH extracted AS (
  SELECT
    DATE "{date}" AS submission_date,
    "{namespace}" AS document_namespace,
    `moz-fx-data-shared-prod`.udf.extract_document_type(_TABLE_SUFFIX) AS document_type,
    `moz-fx-data-shared-prod`.udf.extract_document_version(_TABLE_SUFFIX) AS document_version,
    additional_properties
  FROM
    `moz-fx-data-shared-prod.{namespace}_stable.*`
  WHERE
    -- only observe full days of data
    DATE(submission_timestamp) = date "{date}"
    -- https://cloud.google.com/bigquery/docs/querying-wildcard-tables#filtering_selected_tables_using_table_suffix
    -- IT's also possible to exclude tables in this query e.g
    -- AND _TABLE_SUFFIX NOT IN ('main_v5', 'saved_session_v5', 'first_shutdown_v5')
),
transformed AS (
  SELECT
    * EXCEPT (additional_properties),
    COUNT(*) AS path_count
  FROM
    extracted,
    UNNEST(
      `moz-fx-data-shared-prod`.udf_js.json_extract_missing_cols(
        additional_properties,
        [],
        -- TODO: manually curate known missing schema bits
        []
      )
    ) AS path
  GROUP BY
    submission_date,
    document_namespace,
    document_type,
    document_version,
    path
)
SELECT
  submission_date,
  document_namespace,
  document_type,
  document_version,
  path,
  path_count
FROM
  transformed
"""


def structured_missing_columns(
    date, project, destination_dataset, destination_table, skip_exceptions
):
    """Get missing columns for structured ingestion."""
    client = bigquery.Client(project=project)
    print("Getting the list of namespaces...")
    namespaces = [
        row.schema_name.split("_stable")[0]
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
            mb_processed[namespace] = round(job.total_bytes_processed / 1024 ** 2, 1)
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
    parser.add_argument("--destination-table", default="structured_missing_columns_v1")
    parser.add_argument(
        "--skip-exceptions",
        action="store_true",
        default=False,
        help="continue when running into errors during query time",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    structured_missing_columns(
        args.date,
        args.project,
        args.destination_dataset,
        args.destination_table,
        args.skip_exceptions,
    )
