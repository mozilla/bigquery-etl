-- Generated via bigquery_etl.glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.monitor_cirrus.events_stream`
AS
SELECT
  COALESCE(event_id, CONCAT(document_id, '-', document_event_number)) AS event_id,
  * EXCEPT (event_id),
FROM
  `moz-fx-data-shared-prod.monitor_cirrus_derived.events_stream_v1`
