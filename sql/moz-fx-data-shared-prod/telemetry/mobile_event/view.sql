CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.mobile_event`
AS
SELECT
  * REPLACE (
    -- The pipeline ensures lowercase client_id since 2020-01-10, but we apply
    -- LOWER here to provide continuity for older data that still contains
    -- some uppercase IDs; see https://github.com/mozilla/gcp-ingestion/pull/1069
    LOWER(client_id) AS client_id,
    `moz-fx-data-shared-prod.udf.normalize_metadata`(metadata) AS metadata
  )
FROM
  `moz-fx-data-shared-prod.telemetry_stable.mobile_event_v1`
