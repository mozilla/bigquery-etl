"""Generate a query for unifying legacy metrics and glean metrics.

This approach is required because the order of columns is significant in order
to be able to union two tables, even if the set of columns are the same."""

import requests
import json
from bigquery_etl.format_sql.formatter import reformat
from google.cloud import bigquery


def get_columns(schema):
    """Return a list of columns corresponding to the schema.

    Modified from https://github.com/mozilla-services/mozilla-pipeline-schemas/blob/c2dae92a7ed73e4774a897bc4d2a6a8608875339/mozilla_pipeline_schemas/utils.py#L30-L43
    """

    def traverse(prefix, columns):
        res = []
        for node in columns:
            name = node["name"]
            dtype = node["type"]
            # we consider a repeated field a leaf node, and sorted for our purposes
            if dtype == "RECORD" and node["mode"] != "REPEATED":
                res += traverse(f"{prefix}.{name}", node["fields"])
            else:
                res += [f"{prefix}.{name} {dtype}"]
        return res

    res = traverse("root", schema)
    return sorted(res)


def generate_query(columns, table, submission_date):
    formatted_columns = ",\n".join(columns)
    return reformat(
        f"""
        select {formatted_columns}
        from `{table}`
        where date(submission_timestamp) = "{submission_date}"
    """
    )


def main():
    # get the most schema deploy (to the nearest 15 minutes)
    deploys_url = (
        "https://protosaur.dev/mps-deploys/data/mozilla_pipeline_schemas/deploys.json"
    )
    resp = requests.get(deploys_url)
    deploys_data = resp.json()
    # get the last element that has reached production
    last_prod_deploy = [
        row
        for row in sorted(deploys_data, key=lambda x: x["submission_timestamp"])
        if row["project"] == "moz-fx-data-shared-prod"
    ][-1]
    print(f"last deploy: {last_prod_deploy}")

    # get the schema corresponding to the last commit
    commit_hash = last_prod_deploy["commit_hash"]
    schema_url = (
        "https://raw.githubusercontent.com/mozilla-services/mozilla-pipeline-schemas/"
        f"{commit_hash}/schemas/org-mozilla-ios-firefox/events/events.1.bq"
    )
    resp = requests.get(schema_url)
    schema = resp.json()
    column_summary = get_columns(schema)

    # print(json.dumps(column_summary, indent=2))
    """
    The columns take on the following form:

    "root.additional_properties STRING",
    "root.client_info.android_sdk_version STRING",
    "root.client_info.app_build STRING",
    ...

    This will need to be processed yet again so we can query via bigquery
    """

    # TODO: update schema for the legacy table
    bq = bigquery.Client()
    legacy_table = "moz-fx-data-shared-prod.org_mozilla_ios_firefox_derived.legacy_metrics_v1"
    table = bq.get_table(legacy_table)
    table.schema = schema
    bq.update_table(legacy_table, ["schema"])

    stripped = [c.split()[0].lstrip("root.") for c in column_summary]
    query_glean = generate_query(
        ['"glean" as telemetry_system', *stripped],
        "mozdata.org_mozilla_ios_firefox.metrics",
        "2021-02-01",
    )
    query_legacy = generate_query(
        ['"legacy" as telemetry_system', *stripped],
        legacy_table,
        "2021-02-01",
    )
    view_body = reformat(f"{query_glean} UNION ALL {query_legacy}")
    print(view_body)


if __name__ == "__main__":
    main()
