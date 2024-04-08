-- Generated via ./bqetl generate glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_reality_pc.events_stream`
AS
SELECT
  "org_mozilla_firefoxreality" AS normalized_app_id,
  e.*
FROM
  `moz-fx-data-shared-prod.org_mozilla_firefoxreality.events_stream` AS e
