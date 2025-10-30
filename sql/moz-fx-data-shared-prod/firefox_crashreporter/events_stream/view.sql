-- Generated via bigquery_etl.glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_crashreporter.events_stream`
AS
SELECT
  *,
  STRUCT(
    STRUCT(
      JSON_VALUE(event_extra.id) AS `id`,
      JSON_VALUE(event_extra.reason) AS `reason`
    ) AS `string`
  ) AS extras
FROM
  `moz-fx-data-shared-prod.firefox_crashreporter_derived.events_stream_v1`
