-- Generated via bigquery_etl.glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.ads_backend.events_stream`
AS
SELECT
  *,
  STRUCT(STRUCT(JSON_VALUE(event_extra.flight_id) AS `flight_id`) AS `string`) AS extras
FROM
  `moz-fx-data-shared-prod.ads_backend_derived.events_stream_v1`
