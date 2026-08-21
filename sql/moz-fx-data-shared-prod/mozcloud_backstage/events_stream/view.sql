-- Generated via ./bqetl generate glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.mozcloud_backstage.events_stream`
AS
WITH events_stream_union AS (
  SELECT
    "moz_backstage" AS normalized_app_id,
    e.*
  FROM
    `moz-fx-data-shared-prod.moz_backstage_derived.events_stream_v1` AS e
)
SELECT
  COALESCE(event_id, CONCAT(document_id, '-', document_event_number)) AS event_id,
  * EXCEPT (event_id),
  STRUCT(
    STRUCT(LAX_INT64(event_extra.time_saved) AS `time_saved`) AS `quantity`,
    STRUCT(
      JSON_VALUE(event_extra.entity_ref) AS `entity_ref`,
      JSON_VALUE(event_extra.id) AS `id`,
      JSON_VALUE(event_extra.label) AS `label`,
      JSON_VALUE(event_extra.name) AS `name`,
      JSON_VALUE(event_extra.referrer) AS `referrer`,
      JSON_VALUE(event_extra.title) AS `title`,
      JSON_VALUE(event_extra.type) AS `type`,
      JSON_VALUE(event_extra.url) AS `url`
    ) AS `string`
  ) AS extras
FROM
  events_stream_union
