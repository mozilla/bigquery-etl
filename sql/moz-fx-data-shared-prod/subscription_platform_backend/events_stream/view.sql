-- Generated via bigquery_etl.glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.subscription_platform_backend.events_stream`
AS
SELECT
  COALESCE(event_id, CONCAT(document_id, '-', document_event_number)) AS event_id,
  * EXCEPT (event_id),
  STRUCT(
    STRUCT(
      LAX_BOOL(
        event_extra.subscription_voluntary_cancellation
      ) AS `subscription_voluntary_cancellation`
    ) AS `boolean`,
    STRUCT(
      JSON_VALUE(event_extra.error_reason) AS `error_reason`,
      JSON_VALUE(event_extra.nimbus_user_id) AS `nimbus_user_id`,
      JSON_VALUE(event_extra.subscription_billing_country) AS `subscription_billing_country`,
      JSON_VALUE(
        event_extra.subscription_cancellation_reason
      ) AS `subscription_cancellation_reason`,
      JSON_VALUE(event_extra.trial_conversion_status) AS `trial_conversion_status`
    ) AS `string`
  ) AS extras
FROM
  `moz-fx-data-shared-prod.subscription_platform_backend_derived.events_stream_v1`
