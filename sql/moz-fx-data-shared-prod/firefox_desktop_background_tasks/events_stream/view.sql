-- Generated via bigquery_etl.glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_desktop_background_tasks.events_stream`
AS
SELECT
  COALESCE(event_id, CONCAT(document_id, '-', document_event_number)) AS event_id,
  * EXCEPT (event_id),
  STRUCT(
    STRUCT(
      LAX_BOOL(event_extra.blocked) AS `blocked`,
      LAX_BOOL(event_extra.enabled) AS `enabled`,
      LAX_BOOL(event_extra.result_is_default) AS `result_is_default`,
      LAX_BOOL(event_extra.sampled_in) AS `sampled_in`,
      LAX_BOOL(event_extra.success) AS `success`
    ) AS `boolean`,
    STRUCT(LAX_INT64(event_extra.session_seq) AS `session_seq`) AS `quantity`,
    STRUCT(
      JSON_VALUE(event_extra.action) AS `action`,
      JSON_VALUE(event_extra.feature) AS `feature`,
      JSON_VALUE(event_extra.id) AS `id`,
      JSON_VALUE(event_extra.method) AS `method`,
      JSON_VALUE(event_extra.name) AS `name`,
      JSON_VALUE(event_extra.reason) AS `reason`,
      JSON_VALUE(event_extra.selection) AS `selection`,
      JSON_VALUE(event_extra.session_id) AS `session_id`,
      JSON_VALUE(event_extra.session_start_time) AS `session_start_time`,
      JSON_VALUE(event_extra.value) AS `value`
    ) AS `string`
  ) AS extras
FROM
  `moz-fx-data-shared-prod.firefox_desktop_background_tasks_derived.events_stream_v1`
