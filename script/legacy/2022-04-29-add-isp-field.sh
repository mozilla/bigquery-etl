#!/bin/bash

# It's rare that we need to add a new field to the generated glean ETL for baseline_clients_daily
# but it's fiddly when we do. There are dozens of tables affected and our dependency resolution for
# schema updates with downstream detection is insufficient to cross the per-addId to per-app boundary.
# As such, it's practical to run a custom script instead to inject the schema of affected tables.

# For context, see:
# https://github.com/mozilla/bigquery-etl/pull/2928
# https://bugzilla.mozilla.org/show_bug.cgi?id=1757216


# This script assumes that the following has been run locally:

# bqetl generate glean_usage
# bqetl query schema update 'moz-fx-data-shared-prod.*.baseline_clients_daily_v1'
# bqetl query schema update 'moz-fx-data-shared-prod.*.baseline_clients_last_seen_v1'
# bqetl query schema update 'moz-fx-data-shared-prod.*.clients_last_seen_joined_v1'


# Function to list out all target tables in project:dataset.table format
tables_to_modify() {
    find . -name query.sql | sort \
        | grep -v telemetry_derived \
        | grep -E 'baseline_clients_daily_v1|baseline_clients_last_seen_v1|clients_last_seen_joined_v1' \
        | sed 's,./sql/\(.*\)/\(.*\)/\(.*\)/query.sql,\1:\2.\3,'
}

for t in $(tables_to_modify)
do
    echo "Processing $t"

    # Note that if we accidentally run this twice, the bq update should fail with message
    # "BigQuery error in update operation: Field isp already exists in schema"
    bq show --schema "$t" \
        | jq '. + [{"name":"isp","type":"STRING","mode":"NULLABLE"}]' \
        | bq update --schema /dev/stdin "$t"
done
