#!/usr/bin/env python3

"""Compare number of document IDs in structured decoded, live, and stable tables."""

import datetime
from argparse import ArgumentParser
from collections import defaultdict

from google.cloud import bigquery

DECODED_QUERY = """
WITH decoded AS (
  SELECT 
    * EXCEPT (metadata),  -- Some tables have different field order in metadata
    _TABLE_SUFFIX,
  FROM
    `moz-fx-data-shared-prod.payload_bytes_decoded.structured_*`
  UNION ALL
  SELECT
    * EXCEPT (metadata),
    _TABLE_SUFFIX,
  FROM
    `moz-fx-data-shared-prod.payload_bytes_decoded.stub_installer_*` 
)
SELECT
  DATE(submission_timestamp) AS submission_date,
  SPLIT(_TABLE_SUFFIX, '__')[OFFSET(0)] AS namespace,
  SPLIT(_TABLE_SUFFIX, '__')[OFFSET(1)] AS doc_type,
  COUNT(DISTINCT(document_id)) AS docid_count,
FROM
  decoded
WHERE
  DATE(submission_timestamp) = '{date}'
GROUP BY
  namespace,
  doc_type,
  submission_date
"""

LIVE_STABLE_QUERY = """
SELECT
  DATE(submission_timestamp) AS submission_date,
  _TABLE_SUFFIX AS doc_type,
  COUNT(DISTINCT(document_id)) AS docid_count,
FROM
  `moz-fx-data-shared-prod.{namespace}_{type}.*`
WHERE
  DATE(submission_timestamp) = '{date}'
GROUP BY
  doc_type,
  submission_date
"""

EXCLUDED_NAMESPACES = {"xfocsp_error_report"}  # restricted access


def get_docid_counts(date, project, destination_dataset, destination_table):
    """Get distinct docid count for decoded, live, and stable tables.

    Results are saved to bigquery where each row contains the decoded, live,
    and stable docid counts for each namespace and doc type combination.
    """
    client = bigquery.Client(project=project)

    # key is tuple of (namespace, doc_type), value is dict where key is
    # table type (live, stable, decoded) and value is docid count
    docid_counts_by_doc_type_by_table = defaultdict(dict)

    namespaces = set()

    print("Getting decoded table doc id counts")
    decoded_query_results = client.query(DECODED_QUERY.format(date=date)).result()

    # Get docid counts in decoded tables
    for row in decoded_query_results:
        if row["namespace"] in EXCLUDED_NAMESPACES:
            continue

        docid_counts_by_doc_type_by_table[(row["namespace"], row["doc_type"])][
            "decoded"
        ] = row["docid_count"]

        namespaces.add(row["namespace"])

    # Get docid counts in stable and live tables
    for namespace in namespaces:
        print(f"Getting {namespace} stable doc id counts")
        stable_query_results = client.query(
            LIVE_STABLE_QUERY.format(date=date, namespace=namespace, type="stable")
        ).result()

        for row in stable_query_results:
            docid_counts_by_doc_type_by_table[(namespace, row["doc_type"])][
                "stable"
            ] = row["docid_count"]

        print(f"Getting {namespace} live doc id counts")
        live_query_results = client.query(
            LIVE_STABLE_QUERY.format(date=date, namespace=namespace, type="live")
        ).result()

        for row in live_query_results:
            docid_counts_by_doc_type_by_table[(namespace, row["doc_type"])][
                "live"
            ] = row["docid_count"]

    # Transform into format for bigquery load
    output_data = []
    for (namespace, doctype), data in docid_counts_by_doc_type_by_table.items():
        data["submission_date"] = str(date)
        data["namespace"] = namespace
        data["doc_type"] = doctype
        output_data.append(data)

    load_config = bigquery.LoadJobConfig(
        time_partitioning=bigquery.TimePartitioning(field="submission_date"),
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    load_job = client.load_table_from_json(
        json_rows=output_data,
        destination=f"{destination_dataset}.{destination_table}"
        f"${date.strftime('%Y%m%d')}",
        job_config=load_config,
    )

    load_job.result()


def parse_args():
    """Parse command line arguments."""
    parser = ArgumentParser(description=__doc__)
    parser.add_argument("--date", type=datetime.date.fromisoformat, required=True)
    parser.add_argument("--project", default="moz-fx-data-shared-prod")
    parser.add_argument("--destination-dataset", default="monitoring_derived")
    parser.add_argument("--destination-table", default="structured_distinct_docids_v1")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    get_docid_counts(
        args.date, args.project, args.destination_dataset, args.destination_table,
    )
