CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.amo_glean.firefox_desktop_addons_by_client`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.amo_glean_derived.firefox_desktop_addons_by_client_v1`
