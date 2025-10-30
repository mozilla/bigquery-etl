-- Generated via bigquery_etl.glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.experimenter_backend.events_stream`
AS
SELECT
  *,
  STRUCT(STRUCT(JSON_VALUE(event_extra.nimbus_user_id) AS `nimbus_user_id`) AS `string`) AS extras
FROM
  `moz-fx-data-shared-prod.experimenter_backend_derived.events_stream_v1`
