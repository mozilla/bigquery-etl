#!/bin/bash

## Affected period to delete is
## 2019-05-04T11:00:00Z to 2019-05-11T11:00:00Z

function delete_partitions() {
    local TABLE=$1
    echo bq rm -f -t "$TABLE"'$20190504'
    echo bq rm -f -t "$TABLE"'$20190505'
}

## Tables populated by Dataflow jobs
delete_partitions moz-fx-data-shar-nonprod-efed.activity_stream.impression_stats_v1
delete_partitions moz-fx-data-shar-nonprod-efed.activity_stream.spoc_fills_v1

## Derived tables specific to GCP
delete_partitions moz-fx-data-derived-datasets.clients_last_seen_v1

## Tables copied from Parquet
# Matches first appendix from the Google Doc (which lists AWS derived datasets to delete)
delete_partitions moz-fx-data-derived-datasets.events_v1
delete_partitions moz-fx-data-derived-datasets.first_shutdown_summary_v4
delete_partitions moz-fx-data-derived-datasets.main_summary_v3
delete_partitions moz-fx-data-derived-datasets.main_summary_v4
delete_partitions moz-fx-data-derived-datasets.addons_v2
delete_partitions moz-fx-data-derived-datasets.addons_aggregates_v2
delete_partitions moz-fx-data-derived-datasets.sync_summary_v2
delete_partitions moz-fx-data-derived-datasets.sync_flat_summary_v1
delete_partitions moz-fx-data-derived-datasets.sync_events_v1
delete_partitions moz-fx-data-derived-datasets.telemetry.clients_daily_v6
