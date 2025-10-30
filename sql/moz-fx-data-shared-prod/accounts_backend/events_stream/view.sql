-- Generated via bigquery_etl.glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.accounts_backend.events_stream`
AS
SELECT
  *,
  STRUCT(
    STRUCT(LAX_BOOL(event_extra.linking) AS `linking`) AS `boolean`,
    STRUCT(
      JSON_VALUE(event_extra.error_code) AS `error_code`,
      JSON_VALUE(event_extra.reason) AS `reason`,
      JSON_VALUE(event_extra.scopes) AS `scopes`,
      JSON_VALUE(event_extra.type) AS `type`
    ) AS `string`
  ) AS extras
FROM
  `moz-fx-data-shared-prod.accounts_backend_derived.events_stream_v1`
