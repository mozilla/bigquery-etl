-- Generated via bigquery_etl.glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.monitor_backend.events_stream`
AS
SELECT
  *,
  STRUCT(
    STRUCT(
      JSON_VALUE(event_extra.path) AS `path`,
      JSON_VALUE(event_extra.referrer) AS `referrer`,
      JSON_VALUE(event_extra.utm_campaign) AS `utm_campaign`,
      JSON_VALUE(event_extra.utm_content) AS `utm_content`,
      JSON_VALUE(event_extra.utm_medium) AS `utm_medium`,
      JSON_VALUE(event_extra.utm_source) AS `utm_source`,
      JSON_VALUE(event_extra.utm_term) AS `utm_term`
    ) AS `string`
  ) AS extras
FROM
  `moz-fx-data-shared-prod.monitor_backend_derived.events_stream_v1`
