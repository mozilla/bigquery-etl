### Normalized app and os name for mobile search related tables

Takes app name and os as input :
Returns a struct of normalized_app_name and normalized_app_name_os based on [discussion provided here](https://mozilla-hub.atlassian.net/browse/RS-805)

This output is consumed by the mobile search views (`search.mobile_search_clients_daily`, `search.mobile_search_clients_engines_sources_daily`, and `search.mobile_search_aggregates`) to compute metrics such as `search_count`, `tagged_sap`, and `ad_click` across Fenix, Focus, and Klar.

Since 2024-08-01, these views union two derived tables split at the 2024-07-31 boundary: historical data built from the Glean `metrics` ping (`search_derived.mobile_search_clients_daily_historical_pre202408`), and current data generated via `sql_generators/search_v2` from the Glean `baseline` ping (the corresponding `_v2` derived tables).
