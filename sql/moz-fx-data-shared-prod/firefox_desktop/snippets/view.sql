CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_desktop.snippets`
AS
SELECT
  * REPLACE (mozfun.norm.metadata(metadata) AS metadata)
FROM
  `moz-fx-data-shared-prod.firefox_desktop_derived.snippets_v2`
