-- Generated via bigquery_etl.glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.accounts_cirrus.events_stream`
AS
SELECT
  COALESCE(event_id, CONCAT(document_id, '-', document_event_number)) AS event_id,
  * EXCEPT (event_id),
  STRUCT(
    STRUCT(LAX_BOOL(event_extra.sampled_in) AS `sampled_in`) AS `boolean`,
    STRUCT(LAX_INT64(event_extra.session_seq) AS `session_seq`) AS `quantity`,
    STRUCT(
      JSON_VALUE(event_extra.reason) AS `reason`,
      JSON_VALUE(event_extra.session_id) AS `session_id`,
      JSON_VALUE(event_extra.session_start_time) AS `session_start_time`
    ) AS `string`
  ) AS extras
FROM
  `moz-fx-data-shared-prod.accounts_cirrus_derived.events_stream_v1`
