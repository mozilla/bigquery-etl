CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_desktop.fog_validation`
AS SELECT
  * REPLACE(
    mozfun.norm.metadata(metadata) AS metadata)
FROM
  `moz-fx-data-shared-prod.firefox_desktop_stable.fog_validation_v1`
