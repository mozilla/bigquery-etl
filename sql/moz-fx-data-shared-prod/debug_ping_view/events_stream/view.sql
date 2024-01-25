-- Generated via bigquery_etl.glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.debug_ping_view.events_stream`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.debug_ping_view_derived.events_stream_v1`
