-- Generated via ./bqetl generate glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.mozregression.events_stream`
AS
WITH events_stream_union AS (
  SELECT
    "org_mozilla_mozregression" AS normalized_app_id,
    e.*
  FROM
    `moz-fx-data-shared-prod.org_mozilla_mozregression_derived.events_stream_v1` AS e
)
SELECT
  COALESCE(event_id, CONCAT(document_id, '-', document_event_number)) AS event_id,
  * EXCEPT (event_id),
  STRUCT(
    STRUCT(LAX_BOOL(event_extra.sampled_in) AS `sampled_in`) AS `boolean`,
    STRUCT(LAX_INT64(event_extra.session_seq) AS `session_seq`) AS `quantity`,
    STRUCT(
      JSON_VALUE(event_extra.reason) AS `reason`,
      JSON_VALUE(event_extra.session_id) AS `session_id`,
      JSON_VALUE(event_extra.session_start_time) AS `session_start_time`
    ) AS `string`
  ) AS extras
FROM
  events_stream_union
