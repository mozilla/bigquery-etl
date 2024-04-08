-- Generated via ./bqetl generate glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.lockwise_ios.events_stream`
AS
SELECT
  "org_mozilla_ios_lockbox" AS normalized_app_id,
  e.*
FROM
  `moz-fx-data-shared-prod.org_mozilla_ios_lockbox.events_stream` AS e
