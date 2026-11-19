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
  event_id,
  * EXCEPT (event_id),
  STRUCT(
    STRUCT(LAX_BOOL(event_extra.sampled_in) AS `sampled_in`) AS `boolean`,
    STRUCT(
      LAX_INT64(event_extra.flow_seq) AS `flow_seq`,
      LAX_INT64(event_extra.session_seq) AS `session_seq`
    ) AS `quantity`,
    STRUCT(
      JSON_VALUE(event_extra.flow_name) AS `flow_name`,
      JSON_VALUE(event_extra.reason) AS `reason`,
      JSON_VALUE(event_extra.session_id) AS `session_id`,
      JSON_VALUE(event_extra.session_start_time) AS `session_start_time`,
      JSON_VALUE(event_extra.terminating_event) AS `terminating_event`
    ) AS `string`
  ) AS extras
FROM
  events_stream_union
