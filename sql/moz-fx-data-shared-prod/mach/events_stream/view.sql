-- Generated via ./bqetl generate glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.mach.events_stream`
AS
WITH events_stream_union AS (
  SELECT
    "mozilla_mach" AS normalized_app_id,
    e.*
  FROM
    `moz-fx-data-shared-prod.mozilla_mach_derived.events_stream_v1` AS e
)
SELECT
  CONCAT(document_id, '-', document_event_number) AS event_id,
  *,
FROM
  events_stream_union
