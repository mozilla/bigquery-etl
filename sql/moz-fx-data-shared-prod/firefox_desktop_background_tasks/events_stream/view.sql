-- Generated via bigquery_etl.glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_desktop_background_tasks.events_stream`
AS
SELECT
  *,
  STRUCT(
    STRUCT(
      JSON_VALUE(event_extra.action) AS `action`,
      JSON_VALUE(event_extra.id) AS `id`,
      JSON_VALUE(event_extra.name) AS `name`,
      JSON_VALUE(event_extra.reason) AS `reason`,
      JSON_VALUE(event_extra.value) AS `value`
    ) AS `string`
  ) AS extras
FROM
  `moz-fx-data-shared-prod.firefox_desktop_background_tasks_derived.events_stream_v1`
