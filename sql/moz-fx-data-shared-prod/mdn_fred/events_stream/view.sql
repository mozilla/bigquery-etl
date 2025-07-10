-- Generated via bigquery_etl.glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.mdn_fred.events_stream`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.mdn_fred_derived.events_stream_v1`
