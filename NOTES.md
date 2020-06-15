# Notes

Let's build a schema spider. This should provide some data that can be used to
make decisions around data catalog. The output of this proces will a dependency
graph of tables in BigQuery across projects.

```bash
bq ls --format=json moz-fx-data-shared-prod:telemetry_derived | jq '.' > data/telemetry_derived.listing.json

cat data/telemetry_derived.listing.json | jq '.[] | select(.tableReference.tableId=="core_clients_daily_v1")'
{
  "kind": "bigquery#table",
  "creationTime": "1574172370749",
  "tableReference": {
    "projectId": "moz-fx-data-shared-prod",
    "tableId": "core_clients_daily_v1",
    "datasetId": "telemetry_derived"
  },
  "timePartitioning": {
    "field": "submission_date",
    "type": "DAY"
  },
  "type": "TABLE",
  "id": "moz-fx-data-shared-prod:telemetry_derived.core_clients_daily_v1"
}

# this doesnt any useful information because it's a table
bq query --dry_run --use_legacy_sql=false --format=json \
    'select * from `moz-fx-data-shared-prod`.telemetry_derived.core_clients_daily_v1
    where submission_date = date_sub(current_date, interval 1 day)' | jq

# but maybe we can dereference a view?
cat data/telemetry_derived.listing.json | jq '[.[] | select(.type=="VIEW")][0]'
{
  "kind": "bigquery#table",
  "creationTime": "1592075573176",
  "tableReference": {
    "projectId": "moz-fx-data-shared-prod",
    "tableId": "core_live",
    "datasetId": "telemetry_derived"
  },
  "type": "VIEW",
  "id": "moz-fx-data-shared-prod:telemetry_derived.core_live",
  "view": {
    "useLegacySql": false
  }
}

bq show --format=json moz-fx-data-shared-prod:telemetry_derived.core_live
```

It looks like the json data returned by the listing is not enough to gather
information about where they come from. It does give us information about the
nodes in the graph. We'll need to take a look at bigquery-etl and the query logs
to determine the links between the graphs. What we'll need to do however is to
derefernce any views that are created.

To deference views, we can take a look at the [information schema containg view
metadata](https://cloud.google.com/bigquery/docs/view-metadata). This

```bash
bq query --use_legacy_sql=false 'select * from `moz-fx-data-shared-prod`.telemetry_derived.INFORMATION_SCHEMA.VIEWS limit 3'
+-------------------------+-------------------+-------------------------+-------------------------------------------------------------------------------+--------------+------------------+
|      table_catalog      |   table_schema    |       table_name        |                                view_definition                                | check_option | use_standard_sql |
+-------------------------+-------------------+-------------------------+-------------------------------------------------------------------------------+--------------+------------------+
| moz-fx-data-shared-prod | telemetry_derived | ssl_ratios_v1           | SELECT * FROM `mozilla-public-data.telemetry_derived.ssl_ratios_v1`           | NULL         | YES              |
| moz-fx-data-shared-prod | telemetry_derived | italy_covid19_outage_v1 | SELECT * FROM `mozilla-public-data.telemetry_derived.italy_covid19_outage_v1` | NULL         | YES              |
| moz-fx-data-shared-prod | telemetry_derived | origin_content_blocking | select * from `moz-fx-prio-admin-prod-098j.telemetry.origin_content_blocking` | NULL         | YES              |
+-------------------------+-------------------+-------------------------+-------------------------------------------------------------------------------+--------------+------------------+
```

We'll dry-run each of these to obtain a dereference of the views. We'll ignore
any views we encounter from bigquery-etl to reduce duplicate work, since these
are sources of truth.

```bash
bq query --format=json --use_legacy_sql=false \
    'select * from `moz-fx-data-shared-prod`.telemetry_derived.INFORMATION_SCHEMA.VIEWS' \
    | jq > data/telemetry_derived.view_listing.json

bq query --dry_run --use_legacy_sql=false --format=json \
    $(cat data/telemetry_derived.view_listing.json | jq -r '.[0].view_definition') \
    | jq
{
  "status": {
    "state": "DONE"
  },
  "kind": "bigquery#job",
  "statistics": {
    "query": {
      "totalBytesProcessedAccuracy": "PRECISE",
      "statementType": "SELECT",
      "totalBytesBilled": "0",
      "totalBytesProcessed": "33089028",
      "cacheHit": false,
      "referencedTables": [
        {
          "projectId": "mozilla-public-data",
          "tableId": "ssl_ratios_v1",
          "datasetId": "telemetry_derived"
        }
      ],
      "schema": { ... },
    "creationTime": "1592247616179",
    "totalBytesProcessed": "33089028"
  },
  "jobReference": {
    "projectId": "etl-graph",
    "location": "US"
  },
  "etag": "0R22JEj0sN6llLaF89uLYQ==",
  "selfLink": "https://bigquery.googleapis.com/bigquery/v2/projects/etl-graph/jobs/?location=US",
  "configuration": {
    "query": {
      "priority": "INTERACTIVE",
      "query": "SELECT * FROM `mozilla-public-data.telemetry_derived.ssl_ratios_v1`",
      "writeDisposition": "WRITE_TRUNCATE",
      "destinationTable": {
        "projectId": "etl-graph",
        "tableId": "anon68f47445ca00f2c4ff0b48b76f19d719765dc279",
        "datasetId": "_03669b2b0ddaee49592cf590b62f5694460e8d68"
      },
      "useLegacySql": false
    },
    "dryRun": true,
    "jobType": "QUERY"
  },
  "id": "etl-graph:US.",
  "user_email": "amiyaguchi@mozilla.com"
}
```

Here, we only care about `.query.statistics.referencedTables`, which we can use
as edges in our graph. Now the question is what to do when we run into a table
glob.

```bash
bq query --dry_run --format=json --use_legacy_sql=false \
    'select submission_timestamp from `moz-fx-data-shared-prod.payload_bytes_decoded.telemetry_*`
    where date(submission_timestamp) = date_sub(current_date, interval 1 day)' \
    | jq '.statistics.query.referencedTables'
[
  {
    "projectId": "moz-fx-data-shared-prod",
    "tableId": "telemetry_*",
    "datasetId": "payload_bytes_decoded"
  }
]
```

In order to fully realize edges between views and tables, we'll have to
enumerate all of the nodes and do the globbing ourselves. Thankfully the
globbing rules here are straightforward -- star globs may only be used for the
table suffix.
