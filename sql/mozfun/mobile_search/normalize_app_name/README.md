### Normalized app and os name for mobile search related tables

Takes app name and os as input :
Returns a struct of normalized_app_name and normalized_app_name_os based on [discussion provided here](https://mozilla-hub.atlassian.net/browse/RS-805)

This output is consumed by the mobile search views (`search.mobile_search_clients_daily`, `search.mobile_search_clients_engines_sources_daily`, and `search.mobile_search_aggregates`) to compute metrics such as `search_count`, `tagged_sap`, and `ad_click` across Fenix, Focus, and Klar.
