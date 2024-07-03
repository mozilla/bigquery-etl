#!/usr/bin/env python3

"""
Script for exporting data from client association ping from Firefox Desktop telemetry linked to a specific Account UID.

See https://bugzilla.mozilla.org/show_bug.cgi?id=1895503.
"""

import json
from argparse import ArgumentParser

from google.cloud import bigquery


def _clean_dict(obj):
    """
    Recursively remove keys with value of `None` or empty dictionaries.

    We have a lot of empty columns in the table, this makes the output smaller.
    """
    if isinstance(obj, dict):
        cleaned_dict = {
            k: _clean_dict(v) for k, v in obj.items() if v is not None and v != "None"
        }
        # Remove keys where the value is an empty dictionary
        return {k: v for k, v in cleaned_dict.items() if v}
    elif isinstance(obj, list):
        cleaned_list = [_clean_dict(item) for item in obj]
        # Filter out dictionaries where both 'key' and 'value' are None or empty lists
        cleaned_list = [
            item
            for item in cleaned_list
            if not (
                isinstance(item, dict)
                and item.get("key") is None
                and item.get("value") is None
            )
        ]
    else:
        return str(obj)


def main():
    """Export data, print to STDOUT."""
    parser = ArgumentParser(
        description="Export client association ping data from Firefox Desktop telemetry linked to a specific Account UID."
    )
    parser.add_argument("uid", type=str, help="Account UID to export data for")

    args = parser.parse_args()
    uid = args.uid

    print("Extracting client_association data for: " + uid)

    client = bigquery.Client()
    client_association_query = """
        SELECT
            *
        FROM
            `moz-fx-data-shared-prod.firefox_desktop_stable.fx_accounts_v1`
        WHERE
            DATE(submission_timestamp) >= '2024-06-01'
            AND metrics.string.client_association_uid = @uid
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("uid", "STRING", uid)]
    )
    query_job = client.query(client_association_query, job_config=job_config)
    results = query_job.result()

    rows_as_dicts = [_clean_dict(dict(row)) for row in results]
    json_string = json.dumps(rows_as_dicts)

    print(json_string)


if __name__ == "__main__":
    main()
