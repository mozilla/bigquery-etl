CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_desktop.cfr_users_daily`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.firefox_desktop_derived.cfr_users_daily_v2`
