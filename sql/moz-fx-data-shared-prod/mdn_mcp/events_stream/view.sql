-- Generated via bigquery_etl.glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.mdn_mcp.events_stream`
AS
SELECT
  COALESCE(event_id, CONCAT(document_id, '-', document_event_number)) AS event_id,
  * EXCEPT (event_id),
  STRUCT(
    STRUCT(
      LAX_INT64(event_extra.result_count) AS `result_count`,
      LAX_INT64(event_extra.top_score) AS `top_score`
    ) AS `quantity`,
    STRUCT(
      JSON_VALUE(event_extra.id) AS `id`,
      JSON_VALUE(event_extra.key) AS `key`,
      JSON_VALUE(event_extra.label) AS `label`,
      JSON_VALUE(event_extra.path) AS `path`,
      JSON_VALUE(event_extra.reason) AS `reason`,
      JSON_VALUE(event_extra.referrer) AS `referrer`,
      JSON_VALUE(event_extra.title) AS `title`,
      JSON_VALUE(event_extra.tool) AS `tool`,
      JSON_VALUE(event_extra.top_path) AS `top_path`,
      JSON_VALUE(event_extra.type) AS `type`,
      JSON_VALUE(event_extra.url) AS `url`,
      JSON_VALUE(event_extra.user_agent) AS `user_agent`
    ) AS `string`
  ) AS extras
FROM
  `moz-fx-data-shared-prod.mdn_mcp_derived.events_stream_v1`
