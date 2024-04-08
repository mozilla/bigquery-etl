-- Generated via ./bqetl generate glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.lockwise_android.events_stream`
AS
SELECT
  "mozilla_lockbox" AS normalized_app_id,
  e.*
FROM
  `moz-fx-data-shared-prod.mozilla_lockbox.events_stream` AS e
