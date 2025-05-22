CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_desktop.newtab_clients_daily`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.firefox_desktop_derived.newtab_clients_daily_v2`
