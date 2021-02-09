CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.org_mozilla_reference_browser.events`
AS SELECT
  * REPLACE(
    mozfun.norm.metadata(metadata) AS metadata)
FROM
  `moz-fx-data-shared-prod.org_mozilla_reference_browser_stable.events_v1`
