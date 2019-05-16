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
delete_partitions moz-fx-data-shar-nonprod-efed.activity_stream.impression_stats_v1
delete_partitions moz-fx-data-shar-nonprod-efed.activity_stream.spoc_fills_v1

## Tables copied from Parquet
# Matches first appendix from the Google Doc (which lists AWS derived datasets to delete)
delete_partitions moz-fx-data-derived-datasets.events_v1
delete_partitions moz-fx-data-derived-datasets.experiments_v1
delete_partitions moz-fx-data-derived-datasets.first_shutdown_summary_v4
delete_partitions moz-fx-data-derived-datasets.main_summary_v3
delete_partitions moz-fx-data-derived-datasets.main_summary_v4
delete_partitions moz-fx-data-derived-datasets.addons_v2
delete_partitions moz-fx-data-derived-datasets.addons_aggregates_v2
delete_partitions moz-fx-data-derived-datasets.sync_summary_v2
delete_partitions moz-fx-data-derived-datasets.sync_flat_summary_v1
delete_partitions moz-fx-data-derived-datasets.sync_events_v1
delete_partitions moz-fx-data-derived-datasets.telemetry.clients_daily_v6
delete_partitions moz-fx-data-derived-datasets.search.search_clients_daily_v3
delete_partitions moz-fx-data-derived-datasets.search.search_clients_daily_v4

## Derived tables specific to GCP
# Because clients_last_seen copies client_level data from day to day,
# we have to backfill from the first deleted day up to the present
# to ensure we haven't let data from the deleted period leak and continue
# to be propagated forward.
delete_partitions moz-fx-data-derived-datasets.clients_last_seen_v1

bq query --project moz-fx-data-derived-datasets --nouse_legacy_sql --dataset_id telemetry --destination_table 'clients_last_seen_v1$20190504' --parameter submission_date:DATE:2019-05-04 -n0 -q --replace < sql/clients_last_seen_v1.sql
bq query --project moz-fx-data-derived-datasets --nouse_legacy_sql --dataset_id telemetry --destination_table 'clients_last_seen_v1$20190505' --parameter submission_date:DATE:2019-05-05 -n0 -q --replace < sql/clients_last_seen_v1.sql
bq query --project moz-fx-data-derived-datasets --nouse_legacy_sql --dataset_id telemetry --destination_table 'clients_last_seen_v1$20190506' --parameter submission_date:DATE:2019-05-06 -n0 -q --replace < sql/clients_last_seen_v1.sql
bq query --project moz-fx-data-derived-datasets --nouse_legacy_sql --dataset_id telemetry --destination_table 'clients_last_seen_v1$20190507' --parameter submission_date:DATE:2019-05-07 -n0 -q --replace < sql/clients_last_seen_v1.sql
bq query --project moz-fx-data-derived-datasets --nouse_legacy_sql --dataset_id telemetry --destination_table 'clients_last_seen_v1$20190508' --parameter submission_date:DATE:2019-05-08 -n0 -q --replace < sql/clients_last_seen_v1.sql
bq query --project moz-fx-data-derived-datasets --nouse_legacy_sql --dataset_id telemetry --destination_table 'clients_last_seen_v1$20190509' --parameter submission_date:DATE:2019-05-09 -n0 -q --replace < sql/clients_last_seen_v1.sql
bq query --project moz-fx-data-derived-datasets --nouse_legacy_sql --dataset_id telemetry --destination_table 'clients_last_seen_v1$20190510' --parameter submission_date:DATE:2019-05-10 -n0 -q --replace < sql/clients_last_seen_v1.sql
bq query --project moz-fx-data-derived-datasets --nouse_legacy_sql --dataset_id telemetry --destination_table 'clients_last_seen_v1$20190511' --parameter submission_date:DATE:2019-05-11 -n0 -q --replace < sql/clients_last_seen_v1.sql
bq query --project moz-fx-data-derived-datasets --nouse_legacy_sql --dataset_id telemetry --destination_table 'clients_last_seen_v1$20190512' --parameter submission_date:DATE:2019-05-12 -n0 -q --replace < sql/clients_last_seen_v1.sql
bq query --project moz-fx-data-derived-datasets --nouse_legacy_sql --dataset_id telemetry --destination_table 'clients_last_seen_v1$20190513' --parameter submission_date:DATE:2019-05-13 -n0 -q --replace < sql/clients_last_seen_v1.sql
