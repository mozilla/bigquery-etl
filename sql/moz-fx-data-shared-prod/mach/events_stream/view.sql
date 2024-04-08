-- Generated via ./bqetl generate glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.mach.events_stream`
AS
SELECT
  "mozilla_mach" AS normalized_app_id,
  e.*
FROM
  `moz-fx-data-shared-prod.mozilla_mach.events_stream` AS e
