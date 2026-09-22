-- Generated via bigquery_etl.glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.syncstorage.events_stream`
AS
SELECT
  event_id,
  * EXCEPT (event_id),
FROM
  `moz-fx-data-shared-prod.syncstorage_derived.events_stream_v1`
