CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_desktop.newtab_addon_versions_daily`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.firefox_desktop_derived.newtab_addon_versions_daily_v1`
