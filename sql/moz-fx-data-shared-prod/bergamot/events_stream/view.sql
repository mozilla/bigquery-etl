-- Generated via ./bqetl generate glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.bergamot.events_stream`
AS
SELECT
  "org_mozilla_bergamot" AS normalized_app_id,
  e.*
FROM
  `moz-fx-data-shared-prod.org_mozilla_bergamot.events_stream` AS e
