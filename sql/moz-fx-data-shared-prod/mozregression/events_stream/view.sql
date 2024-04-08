-- Generated via ./bqetl generate glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.mozregression.events_stream`
AS
SELECT
  "org_mozilla_mozregression" AS normalized_app_id,
  e.*
FROM
  `moz-fx-data-shared-prod.org_mozilla_mozregression.events_stream` AS e
