CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_desktop.newtab_visits_daily`
AS
SELECT
  'Firefox Desktop' AS app_name,
  *
FROM
  `moz-fx-data-shared-prod.firefox_desktop_derived.newtab_visits_daily_v2`
