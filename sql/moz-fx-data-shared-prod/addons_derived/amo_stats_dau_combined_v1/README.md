# addons_derived.amo_stats_dau_combined_v1

## Description

Computes amo stats dau using both the legacy source and the new addons ping data by unioning `moz-fx-data-shared-prod.addons_derived.firefox_desktop_addons_by_client_legacy_source_v1` (legacy telemetry based) and `moz-fx-data-shared-prod.addons_derived.fenix_addons_by_client_legacy_source_v1` (metrics based Glean ping based)for data from major version 147 and below, and `moz-fx-data-shared-prod.addons_derived.firefox_desktop_addons_by_client_v1` (addons based Glean ping based) and `moz-fx-data-shared-prod.addons_derived.fenix_addons_by_client_v1` (addons based Glean ping based) for data from major version 148 and above.

This union happens at this stage in order to ensure consistent aggregatation across all fields and easy downstream consumption.
