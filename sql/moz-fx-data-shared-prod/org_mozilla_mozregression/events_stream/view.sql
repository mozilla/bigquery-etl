-- Generated via bigquery_etl.glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.org_mozilla_mozregression.events_stream`
AS
SELECT
  *,
FROM
  `moz-fx-data-shared-prod.org_mozilla_mozregression_derived.events_stream_v1`
