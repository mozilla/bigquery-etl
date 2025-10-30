-- Generated via bigquery_etl.glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.debug_ping_view.events_stream`
AS
SELECT
  *,
  STRUCT(
    STRUCT(
      JSON_VALUE(event_extra.button) AS `button`,
      JSON_VALUE(event_extra.id) AS `id`,
      JSON_VALUE(event_extra.label) AS `label`,
      JSON_VALUE(event_extra.referrer) AS `referrer`,
      JSON_VALUE(event_extra.title) AS `title`,
      JSON_VALUE(event_extra.type) AS `type`,
      JSON_VALUE(event_extra.url) AS `url`
    ) AS `string`
  ) AS extras
FROM
  `moz-fx-data-shared-prod.debug_ping_view_derived.events_stream_v1`
