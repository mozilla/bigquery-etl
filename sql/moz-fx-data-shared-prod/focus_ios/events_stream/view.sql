-- Generated via ./bqetl generate glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.focus_ios.events_stream`
AS
WITH events_stream_union AS (
  SELECT
    "org_mozilla_ios_focus" AS normalized_app_id,
    e.*
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_focus_derived.events_stream_v1` AS e
)
SELECT
  *,
  STRUCT(
    STRUCT(LAX_BOOL(event_extra.is_enabled) AS `is_enabled`) AS `boolean`,
    STRUCT(LAX_INT64(event_extra.current_item) AS `current_item`) AS `quantity`,
    STRUCT(
      JSON_VALUE(event_extra.branch) AS `branch`,
      JSON_VALUE(event_extra.card_type) AS `card_type`,
      JSON_VALUE(event_extra.conflict_slug) AS `conflict_slug`,
      JSON_VALUE(event_extra.engine_name) AS `engine_name`,
      JSON_VALUE(event_extra.error_string) AS `error_string`,
      JSON_VALUE(event_extra.experiment) AS `experiment`,
      JSON_VALUE(event_extra.experiment_type) AS `experiment_type`,
      JSON_VALUE(event_extra.feature_id) AS `feature_id`,
      JSON_VALUE(event_extra.item) AS `item`,
      JSON_VALUE(event_extra.part_id) AS `part_id`,
      JSON_VALUE(event_extra.reason) AS `reason`,
      JSON_VALUE(event_extra.slug) AS `slug`,
      JSON_VALUE(event_extra.source_of_change) AS `source_of_change`,
      JSON_VALUE(event_extra.status) AS `status`,
      JSON_VALUE(event_extra.tracker_changed) AS `tracker_changed`
    ) AS `string`
  ) AS extras
FROM
  events_stream_union
