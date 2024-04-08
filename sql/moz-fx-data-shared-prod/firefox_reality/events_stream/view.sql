-- Generated via ./bqetl generate glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_reality.events_stream`
AS
SELECT
  "org_mozilla_vrbrowser" AS normalized_app_id,
  e.*
FROM
  `moz-fx-data-shared-prod.org_mozilla_vrbrowser.events_stream` AS e
