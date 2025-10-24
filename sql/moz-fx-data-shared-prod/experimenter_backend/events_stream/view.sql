-- Generated via bigquery_etl.glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.experimenter_backend.events_stream`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.experimenter_backend_derived.events_stream_v1`
