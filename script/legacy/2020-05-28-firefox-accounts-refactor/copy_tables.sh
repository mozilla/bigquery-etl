#!/bin/bash

# Copying out of derived-datasets project
bq cp moz-fx-data-derived-datasets:telemetry.fxa_auth_events_v1 moz-fx-data-shared-prod:firefox_accounts_derived.fxa_auth_events_v1
bq cp moz-fx-data-derived-datasets:telemetry.fxa_auth_bounce_events_v1 moz-fx-data-shared-prod:firefox_accounts_derived.fxa_auth_bounce_events_v1
bq cp moz-fx-data-derived-datasets:telemetry.fxa_content_events_v1 moz-fx-data-shared-prod:firefox_accounts_derived.fxa_content_events_v1
bq cp moz-fx-data-derived-datasets:telemetry.fxa_oauth_events_v1 moz-fx-data-shared-prod:firefox_accounts_derived.fxa_oauth_events_v1
bq cp moz-fx-data-derived-datasets:telemetry.fxa_users_daily_v1 moz-fx-data-shared-prod:firefox_accounts_derived.fxa_users_daily_v1
bq cp moz-fx-data-derived-datasets:telemetry.fxa_users_last_seen_raw_v1 moz-fx-data-shared-prod:firefox_accounts_derived.fxa_users_last_seen_v1
bq cp moz-fx-data-derived-datasets:telemetry.firefox_accounts_exact_mau28_raw_v1 moz-fx-data-shared-prod:firefox_accounts_derived.exact_mau28_v1

# Copying from telemetry_derived in shared-prod
bq cp moz-fx-data-shared-prod:telemetry_derived.fxa_users_services_daily_v1 moz-fx-data-shared-prod:firefox_accounts_derived.fxa_users_services_daily_v1
bq cp moz-fx-data-shared-prod:telemetry_derived.fxa_users_services_first_seen_v1 moz-fx-data-shared-prod:firefox_accounts_derived.fxa_users_services_first_seen_v1
bq cp moz-fx-data-shared-prod:telemetry_derived.fxa_users_services_last_seen_v1 moz-fx-data-shared-prod:firefox_accounts_derived.fxa_users_services_last_seen_v1
