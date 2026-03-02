CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.addons.fenix_addons_by_client`
AS
SELECT
  *,
FROM
  `moz-fx-data-shared-prod.addons_derived.fenix_addons_by_client_legacy_source_v1`
