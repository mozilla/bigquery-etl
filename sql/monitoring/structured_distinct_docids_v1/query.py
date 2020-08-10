#!/usr/bin/env python3

"""Compare number of document IDs in structured decoded, live, and stable tables"""

import datetime
from argparse import ArgumentParser
from collections import defaultdict

from google.cloud import bigquery

DECODED_QUERY = """
SELECT
  DATE(submission_timestamp) AS submission_date,
  SPLIT(_TABLE_SUFFIX, '__')[OFFSET(0)] AS namespace,
  SPLIT(_TABLE_SUFFIX, '__')[OFFSET(1)] AS doc_type,
  COUNT(DISTINCT(document_id)) AS docid_count,
FROM
  `moz-fx-data-shared-prod.payload_bytes_decoded.structured_*`
WHERE
  DATE(submission_timestamp) = '{date}'
GROUP BY
  namespace,
  doc_type,
  submission_date
ORDER BY
    namespace,
    doc_type
"""


def main(date, project, destination_dataset, destination_table):
    client = bigquery.Client(project=project)

    # key is tuple of (namespace, doc_type), value is dict where
    # key is table type (live, stable, decoded) and value is docid count
    docid_counts_by_doc_type_by_table = defaultdict(dict)

    decoded_query_job = client.query(DECODED_QUERY.format(date=date))
    decoded_query_results = decoded_query_job.result()

    for row in decoded_query_results:
        docid_counts_by_doc_type_by_table[(row["namespace"], row["doc_type"])][
            "decoded"
        ] = row["docid_count"]

    print(1)


def parse_args():
    parser = ArgumentParser(description=__doc__)
    parser.add_argument("--date", type=datetime.date.fromisoformat, required=True)
    parser.add_argument("--project", default="moz-fx-data-shared-prod")
    parser.add_argument("--destination-dataset", default="monitoring")
    parser.add_argument("--destination-table", default="structured_distinct_docids_v1")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    main(args.date, args.project, args.destination_dataset, args.destination_table)
