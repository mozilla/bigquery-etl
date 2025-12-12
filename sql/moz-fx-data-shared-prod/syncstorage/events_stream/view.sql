-- Generated via bigquery_etl.glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.syncstorage.events_stream`
AS
SELECT
  CONCAT(document_id, '-', document_event_number) AS event_id,
  *,
FROM
  `moz-fx-data-shared-prod.syncstorage_derived.events_stream_v1`
