-- Generated via bigquery_etl.glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.accounts_frontend.events_stream`
AS
SELECT
  *,
  STRUCT(
    STRUCT(LAX_BOOL(event_extra.third_party_links) AS `third_party_links`) AS `boolean`,
    STRUCT(
      JSON_VALUE(event_extra.choice) AS `choice`,
      JSON_VALUE(event_extra.id) AS `id`,
      JSON_VALUE(event_extra.label) AS `label`,
      JSON_VALUE(event_extra.nimbus_user_id) AS `nimbus_user_id`,
      JSON_VALUE(event_extra.reason) AS `reason`,
      JSON_VALUE(event_extra.referrer) AS `referrer`,
      JSON_VALUE(event_extra.title) AS `title`,
      JSON_VALUE(event_extra.type) AS `type`,
      JSON_VALUE(event_extra.url) AS `url`
    ) AS `string`
  ) AS extras
FROM
  `moz-fx-data-shared-prod.accounts_frontend_derived.events_stream_v1`
