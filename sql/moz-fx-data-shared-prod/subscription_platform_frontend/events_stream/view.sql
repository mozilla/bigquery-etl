-- Generated via bigquery_etl.glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.subscription_platform_frontend.events_stream`
AS
SELECT
  CONCAT(document_id, '-', document_event_number) AS event_id,
  *,
  STRUCT(
    STRUCT(
      JSON_VALUE(event_extra.action) AS `action`,
      JSON_VALUE(event_extra.eligibility_status) AS `eligibility_status`,
      JSON_VALUE(event_extra.entrypoint) AS `entrypoint`,
      JSON_VALUE(event_extra.error_reason) AS `error_reason`,
      JSON_VALUE(event_extra.flow_type) AS `flow_type`,
      JSON_VALUE(event_extra.id) AS `id`,
      JSON_VALUE(event_extra.interval) AS `interval`,
      JSON_VALUE(event_extra.label) AS `label`,
      JSON_VALUE(event_extra.nimbus_user_id) AS `nimbus_user_id`,
      JSON_VALUE(event_extra.offering_id) AS `offering_id`,
      JSON_VALUE(event_extra.outcome) AS `outcome`,
      JSON_VALUE(event_extra.page_name) AS `page_name`,
      JSON_VALUE(event_extra.referrer) AS `referrer`,
      JSON_VALUE(event_extra.step) AS `step`,
      JSON_VALUE(event_extra.title) AS `title`,
      JSON_VALUE(event_extra.type) AS `type`,
      JSON_VALUE(event_extra.url) AS `url`,
      JSON_VALUE(event_extra.utm_campaign) AS `utm_campaign`,
      JSON_VALUE(event_extra.utm_medium) AS `utm_medium`,
      JSON_VALUE(event_extra.utm_source) AS `utm_source`
    ) AS `string`
  ) AS extras
FROM
  `moz-fx-data-shared-prod.subscription_platform_frontend_derived.events_stream_v1`
