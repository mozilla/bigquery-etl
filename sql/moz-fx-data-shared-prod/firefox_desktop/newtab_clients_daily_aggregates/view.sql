CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_desktop.newtab_clients_daily_aggregates`
AS
SELECT
  'Firefox Desktop' AS app_name,
  *
FROM
  `moz-fx-data-shared-prod.firefox_desktop_derived.newtab_clients_daily_aggregates_v2`
