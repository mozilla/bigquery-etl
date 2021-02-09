CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.org_mozilla_reference_browser.deletion_request`
AS SELECT
  * REPLACE(
    mozfun.norm.metadata(metadata) AS metadata)
FROM
  `moz-fx-data-shared-prod.org_mozilla_reference_browser_stable.deletion_request_v1`
