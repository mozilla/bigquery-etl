CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.addons.firefox_desktop_addons_by_client`
AS
SELECT
  *,
FROM
  `moz-fx-data-shared-prod.addons_derived.firefox_desktop_addons_by_client_legacy_source_v1`
