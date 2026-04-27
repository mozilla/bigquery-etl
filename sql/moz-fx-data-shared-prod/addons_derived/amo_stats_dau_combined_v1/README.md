# addons_derived.amo_stats_dau_combined_v1

## Description

Computes AMO (addons.mozilla.org) stats DAU using both the legacy source and the new addons ping data by unioning:

- Firefox Desktop legacy telemetry data, includes entries with **major app version below 148** (`moz-fx-data-shared-prod.addons_derived.firefox_desktop_addons_by_client_legacy_source_v1`).
- Firefox Desktop Glean addons data, includes entries with **major app version 148 and above** (`moz-fx-data-shared-prod.addons_derived.firefox_desktop_addons_by_client_v1`).
- Fenix Glean metrics ping for **all Fenix app versions** (`moz-fx-data-shared-prod.addons_derived.fenix_addons_by_client_v1`).

This union happens at this stage in order to ensure consistent aggregatation across all fields and easy downstream consumption.
