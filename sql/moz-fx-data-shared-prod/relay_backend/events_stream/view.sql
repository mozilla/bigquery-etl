-- Generated via bigquery_etl.glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.relay_backend.events_stream`
AS
SELECT
  *,
  STRUCT(
    STRUCT(
      LAX_BOOL(event_extra.created_by_api) AS `created_by_api`,
      LAX_BOOL(event_extra.has_extension) AS `has_extension`,
      LAX_BOOL(event_extra.has_website) AS `has_website`,
      LAX_BOOL(event_extra.is_random_mask) AS `is_random_mask`,
      LAX_BOOL(event_extra.is_reply) AS `is_reply`
    ) AS `boolean`,
    STRUCT(
      LAX_INT64(event_extra.date_got_extension) AS `date_got_extension`,
      LAX_INT64(event_extra.date_joined_premium) AS `date_joined_premium`,
      LAX_INT64(event_extra.date_joined_relay) AS `date_joined_relay`,
      LAX_INT64(event_extra.n_deleted_domain_masks) AS `n_deleted_domain_masks`,
      LAX_INT64(event_extra.n_deleted_random_masks) AS `n_deleted_random_masks`,
      LAX_INT64(event_extra.n_domain_masks) AS `n_domain_masks`,
      LAX_INT64(event_extra.n_random_masks) AS `n_random_masks`
    ) AS `quantity`,
    STRUCT(
      JSON_VALUE(event_extra.endpoint) AS `endpoint`,
      JSON_VALUE(event_extra.fxa_id) AS `fxa_id`,
      JSON_VALUE(event_extra.method) AS `method`,
      JSON_VALUE(event_extra.platform) AS `platform`,
      JSON_VALUE(event_extra.premium_status) AS `premium_status`,
      JSON_VALUE(event_extra.reason) AS `reason`
    ) AS `string`
  ) AS extras
FROM
  `moz-fx-data-shared-prod.relay_backend_derived.events_stream_v1`
