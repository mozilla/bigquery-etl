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
      LAX_BOOL(event_extra.success) AS `success`
    ) AS `boolean`,
    STRUCT(
      JSON_VALUE(event_extra.action) AS `action`,
      JSON_VALUE(event_extra.feature) AS `feature`,
      JSON_VALUE(event_extra.id) AS `id`,
      JSON_VALUE(event_extra.method) AS `method`,
      JSON_VALUE(event_extra.name) AS `name`,
      JSON_VALUE(event_extra.reason) AS `reason`,
      JSON_VALUE(event_extra.selection) AS `selection`,
      JSON_VALUE(event_extra.value) AS `value`
    ) AS `string`
  ) AS extras
FROM
  `moz-fx-data-shared-prod.firefox_desktop_background_tasks_derived.events_stream_v1`
