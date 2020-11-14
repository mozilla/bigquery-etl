#!/usr/bin/env python3

"""Compare number of document IDs in structured decoded, live, and stable tables."""
import datetime
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
    -- IT's also possible to exclude tables in this query
    -- AND _TABLE_SUFFIX NOT IN ('main_v4', 'saved_session_v4', 'first_shutdown_v4')
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

# handy query for table sizes, although some tables should have less additional
# properties than others.
"""
select *, round(byte_size/pow(1024,3), 2) as gb_size
from monitoring.stable_table_sizes_v1
where submission_date = "2020-11-11"
and dataset_id <> "telemetry_stable"
order by gb_size desc
"""


def structured_missing_columns(date, project, destination_dataset, destination_table):
    """Get missing columns for structured ingestion."""
    client = bigquery.Client(project=project)
    print("Getting the list of namespaces...")
    namespaces = [
        row.schema_name.split("_stable")[0]
        for row in client.query(NAMESPACE_QUERY).result()
        # NOTE: making an exception for this namespace, because it's large.
        if row.schema_name != "activity_stream_stable"
    ]
    print(namespaces)

    for i, namespace in enumerate(namespaces[:10]):
        print(f"Running job for {namespace}")
        client.query(
            QUERY.format(date=date, namespace=namespace),
            job_config=bigquery.QueryJobConfig(
                time_partitioning=bigquery.TimePartitioning(field="submission_date"),
                create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
                write_disposition=(
                    bigquery.WriteDisposition.WRITE_TRUNCATE
                    if i == 0
                    else bigquery.WriteDisposition.WRITE_APPEND
                ),
                destination=(
                    f"{project}.{destination_dataset}.{destination_table}"
                    f"${date.strftime('%Y%m%d')}"
                ),
            ),
        ).result()


def parse_args():
    """Parse command line arguments."""
    parser = ArgumentParser(description=__doc__)
    parser.add_argument("--date", type=datetime.date.fromisoformat, required=True)
    parser.add_argument("--project", default="moz-fx-data-shared-prod")
    parser.add_argument("--destination-dataset", default="monitoring")
    parser.add_argument("--destination-table", default="structured_missing_columns_v1")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    structured_missing_columns(
        args.date,
        args.project,
        args.destination_dataset,
        args.destination_table,
    )
