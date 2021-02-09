CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_desktop.baseline`
AS SELECT
  * REPLACE(
    mozfun.norm.metadata(metadata) AS metadata)
FROM
  `moz-fx-data-shared-prod.firefox_desktop_stable.baseline_v1`
