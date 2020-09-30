#!/bin/bash

## Affected period to delete is
## 2019-05-04T11:00:00Z to 2019-05-11T11:00:00Z

function delete_partitions() {
    local TABLE=$1
    bq rm -f -t "$TABLE"'$20190504'
    bq rm -f -t "$TABLE"'$20190505'
    bq rm -f -t "$TABLE"'$20190506'
    bq rm -f -t "$TABLE"'$20190507'
    bq rm -f -t "$TABLE"'$20190508'
    bq rm -f -t "$TABLE"'$20190509'
    bq rm -f -t "$TABLE"'$20190510'
    bq rm -f -t "$TABLE"'$20190511'
}

## Tables populated by Dataflow jobs
delete_partitions moz-fx-data-shar-nonprod-efed:activity_stream.impression_stats_v1
delete_partitions moz-fx-data-shar-nonprod-efed:activity_stream.spoc_fills_v1

## Tables copied from Parquet
# Matches first appendix from the Google Doc (which lists AWS derived datasets to delete)
delete_partitions moz-fx-data-derived-datasets:telemetry.events_v1
delete_partitions moz-fx-data-derived-datasets:telemetry.experiments_v1
delete_partitions moz-fx-data-derived-datasets:telemetry.crash_summary_v1
delete_partitions moz-fx-data-derived-datasets:telemetry.first_shutdown_summary_v4
# No landfill sample in BQ
# No longitudinal in BQ
delete_partitions moz-fx-data-derived-datasets:telemetry.main_summary_v3
delete_partitions moz-fx-data-derived-datasets:telemetry.main_summary_v4
delete_partitions moz-fx-data-derived-datasets:telemetry.addons_v2
delete_partitions moz-fx-data-derived-datasets:telemetry.addons_aggregates_v2
# main_events is another name for events_v1, which is already handled above
# No base sync table in BQ
delete_partitions moz-fx-data-derived-datasets:telemetry.sync_summary_v2
delete_partitions moz-fx-data-derived-datasets:telemetry.sync_flat_summary_v1
delete_partitions moz-fx-data-derived-datasets:telemetry.sync_events_v1
# No clients_daily_v5 in BQ
delete_partitions moz-fx-data-derived-datasets:telemetry.clients_daily_v6
# The dates in question are not present in search_clients_daily_v3
delete_partitions moz-fx-data-derived-datasets:search.search_clients_daily_v4

## Make static copies of derived data without client_id that we want to keep.
bq cp moz-fx-data-derived-datasets:telemetry.'clients_last_seen_v1' moz-fx-data-derived-datasets:static.'archive_20190516_clients_last_seen_v1'
bq cp moz-fx-data-derived-datasets:telemetry.firefox_desktop_exact_mau28_by_dimensions_v1 moz-fx-data-derived-datasets:static.archive_20190516_firefox_desktop_exact_mau28_by_dimensions_v1

## Derived tables specific to GCP
# Because clients_last_seen copies client_level data from day to day,
# we have to backfill from the first deleted day up to the present
# to ensure we haven't let data from the deleted period leak and continue
# to be propagated forward.
delete_partitions moz-fx-data-derived-datasets:telemetry.clients_last_seen_v1
bq rm -f -t 'moz-fx-data-derived-datasets:telemetry.clients_last_seen_v1$20190511'
bq rm -f -t 'moz-fx-data-derived-datasets:telemetry.clients_last_seen_v1$20190512'
bq rm -f -t 'moz-fx-data-derived-datasets:telemetry.clients_last_seen_v1$20190513'
bq rm -f -t 'moz-fx-data-derived-datasets:telemetry.clients_last_seen_v1$20190514'
bq rm -f -t 'moz-fx-data-derived-datasets:telemetry.clients_last_seen_v1$20190515'
delete_partitions moz-fx-data-derived-datasets:telemetry.clients_last_seen_raw_v1
bq rm -f -t 'moz-fx-data-derived-datasets:telemetry.clients_last_seen_raw_v1$20190511'
bq rm -f -t 'moz-fx-data-derived-datasets:telemetry.clients_last_seen_raw_v1$20190512'
bq rm -f -t 'moz-fx-data-derived-datasets:telemetry.clients_last_seen_raw_v1$20190513'
bq rm -f -t 'moz-fx-data-derived-datasets:telemetry.clients_last_seen_raw_v1$20190514'
bq rm -f -t 'moz-fx-data-derived-datasets:telemetry.clients_last_seen_raw_v1$20190515'


./script/generate_incremental_table --destination_table clients_last_seen_v1 --start 2019-05-04 --end 2019-05-15 --dataset=telemetry sql/moz-fx-data-shared-prod/clients_last_seen_v1.sql
./script/generate_incremental_table --destination_table clients_last_seen_raw_v1 --start 2019-05-04 --end 2019-05-15 --dataset=telemetry sql/moz-fx-data-shared-prod/clients_last_seen_raw_v1.sql
