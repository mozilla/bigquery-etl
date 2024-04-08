-- Generated via ./bqetl generate glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.reference_browser.events_stream`
AS
SELECT
  "org_mozilla_reference_browser" AS normalized_app_id,
  e.*
FROM
  `moz-fx-data-shared-prod.org_mozilla_reference_browser.events_stream` AS e
