# Notes

Let's build a network of the relationships between tables in BigQuery. The
catalog of datasets and tables keeps growing, but our understanding of the
relationships between them is opaque. The goal is to explore the different ways
that table relationships can be surfaced.

In this project, we generate an an index of tables that is consumed by graph
visualization software. The methods for creating this dataset can help inform or
automate the discover of relationships between tables.

## Resolving Views

We start by exploring the JSON output from the various `bq` commands. Running
`bq ls` can enumerate both the datasets and tables within a project.

```bash
# Dump a listing of the telemetry_derived, which contains results derived from the telemetry dataset
bq ls --format=json moz-fx-data-shared-prod:telemetry_derived | jq '.' > data/telemetry_derived.listing.json

# Lets take a look at one of the tables.
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

# We can query a single partition using the --dry_run flag. While we get the schema, we dont know
# what query was run to derive it.
bq query --dry_run --use_legacy_sql=false --format=json \
    'select * from `moz-fx-data-shared-prod`.telemetry_derived.core_clients_daily_v1
    where submission_date = date_sub(current_date, interval 1 day)' | jq

# ...but maybe we can dereference a view?
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

The JSON response from both `bq ls` and `bq query --dry_run` gives us all of the
known tables and views in any project that we have access to.
Instead of using the `bq ls` listing, it's more efficient to use
[`{dataset}.INFORMATION_SCHEMA.TABLES` and
`{dataset}.INFORMATION_SCHEMA.VIEWS`](https://cloud.google.com/bigquery/docs/view-metadata)
directly to enumerate tables.

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

We'll dry-run each of these to obtain a dereference of the views. Here's an
example response for reference.

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
globbing rules are straightforward -- star globs may only be used for the
table suffix.

This problem is tricky because we do not have information about the underylying
tables. They may require partition filters. We try various combinations of
queries to over come this. Here are some example errors.

```bash
bq query --use_legacy_sql=false --dry_run --format=json 'select * from `moz-fx-data-shared-prod`.mozza.event'
Error in query string: Cannot query over table 'moz-fx-data-shared-prod.mozza_stable.event_v1' without a filter over column(s) 'submission_timestamp' that can be used for partition elimination

bq query --use_legacy_sql=false --dry_run --format=json 'select * from `moz-fx-data-shared-prod`.mozza.event where submission_date = date_sub(current_date, interval 1 day)'
Error in query string: Unrecognized name: submission_date at [1:59]
```

## Resolving Queries

Now that we have most of the tables and views resolved, we can start to look at
the relationships between them. One way to this is to assume
`mozilla/bigquery-etl` as the source. While this approach is feasible, there are
some quirks related to the behavior of backticks being used as substitition in
the shell.

The approach taken here is to take advantage of bigquery metadata.

```sql
SELECT
  creation_time,
  destination_table,
  referenced_tables
FROM
  `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE
  error_result IS NULL
  AND state="DONE"
  -- dont care about destination tables without references at the moment
  AND referenced_tables IS NOT NULL
  -- filter out queries without a real destination
  AND NOT (destination_table.table_id LIKE "anon%"
    AND destination_table.dataset_id LIKE "_%")
LIMIT
  100
```

There are a few quirks that need to be resolved to obtain a clean list of edges.
First, we'll remove any of the partition filters (e.g.
`baseline_clients_daily_v1$20200511`). We also take care to remove suffixes
added by the bigquery-spark connector (e.g.
`active_profiles_v1_2020_04_19_80b09a`) Then we'll flatten out this list and
deduplicate any of the edges while retaining the most recent creation date. See
`etl-graph/resources/shared_prod_edgelist.sql` for more details.

## Generating edges

Now that we've collected the pre-requisite data from the project, we generate a
JSON and CSV blob that represents the set of edges between tables. The first
column is the destination table of the query, while the second column is the
referenced tables within the query. We merge this with edges from the
derefernced views so we can build a complete picture of the network.

The resulting CSV file can be imported directly into Gephi. The JSON file can be
manipulated within Python or Javascript with relative easy.

## Visualization

The visualization is done within the browser for interactive use. It takes
advantage of `vis-network` which allows for manipulation of the network. There
were a few iterations that were taken on the final structure of the network. The
decision was made to add the qualified dataset name as a node in the network,
since this decreased the number of connected components in the graph. This
overall gives the graph more structure and makes it easy to list the tables
within a dataset. The settings for the visualization have not been tuned very
much, so it does take quite a bit for the network to reach a steady state (if it
does at all).

The site is an html file that refers to the edges in GCS. The site is hosted in
cloud storage, using protodash to proxy the files behind authentication.
Overall, this approach works great for this visualization.

## Areas for further exploration

The current network is built only using table and query information from
`moz-fx-data-shared-prod`. There are several projects that would be useful to
surface, including the public-data dataset and all of the sandbox projects. The
views and tables can be crawled with the current permissions, but there may be
hurdles to get access to `INFORMATION_SCHEMA.JOBS_BY_PROJECT` in other projects
outside of shared-prod.

There is minor enhancements to be made, such as resolving table globs from
views. This should actually resolve to all of the tables that are actually
referenced. Dry run does not expose this information.

While trying to crawl queries from within `bigquery-etl`, it was noticably
difficult to `--dry-run` queries within the repository. While the query logs are
useful, being able to map these to the `bigquery-etl` queries would be
beneifical for pointing toward the location where the query can be modified.
Additionally, it is difficult to infer the destination table location from the
`bigquery-etl` repository because the project is not encoded within the path to
the SQL files. It is worth considering refactoring the ETL repository to make
the destination table more obvious and easier to map to the query logs.

The visualization could be improved on a bit. It is noisy since there are many
edges added for datasets with many tables. One feature that would be useful
would be to dynamically prune the network so you only see a local view of the
parents and dependents of a node. The network looks (and is) complex, so this
would make navigation and responsiveness much better.

The network currently throws away weights related to the edges. A single table
may point to another table several times because a query is being run on a
schedule as opposed to on demand. The edge could include the count of the times
that it appears, along with the last run time. The network continues to evolve
over time, which can be surfaced by changing attributes in the graph such as the
size of a node or the thickness of its edges. These attributes can also affect
the physics simulation that is used to visualize the network.

Scheduling the crawler should be straightforward. Serving a single JSON file or
a table in BigQuery would be an ideal output. This could be consumed various
tools.

## Conclusions

With the interest in building out tooling to make schemas and table provenance
easier to reason about, there is a clear path to reasoning about relationships
between tables. With [bug
1646157](https://bugzilla.mozilla.org/show_bug.cgi?id=1646157), we are now
empowered to use the BigQuery query logs to surface this information. One
example application of this network would be to answer the question "how was
this table generated?" We can surface the last query that generated it, as well
as the tables that it references. In a prototype of a schema dictionary, the
table relationship information can provide navigation between tables. An up to
date index of tables could be useful for exploring our evolving datasets.
