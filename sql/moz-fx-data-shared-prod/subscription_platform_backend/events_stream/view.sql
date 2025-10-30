-- Generated via bigquery_etl.glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.subscription_platform_backend.events_stream`
AS
SELECT
  *,
  STRUCT(
    STRUCT(
      LAX_BOOL(
        event_extra.subscription_voluntary_cancellation
      ) AS `subscription_voluntary_cancellation`
    ) AS `boolean`,
    STRUCT(
      JSON_VALUE(event_extra.subscription_cancellation_reason) AS `subscription_cancellation_reason`
    ) AS `string`
  ) AS extras
FROM
  `moz-fx-data-shared-prod.subscription_platform_backend_derived.events_stream_v1`
