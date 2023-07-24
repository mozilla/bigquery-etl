CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_desktop.cfr`
AS
SELECT
  * REPLACE (mozfun.norm.metadata(metadata) AS metadata)
FROM
  `moz-fx-data-shared-prod.firefox_desktop_derived.cfr_v2`
