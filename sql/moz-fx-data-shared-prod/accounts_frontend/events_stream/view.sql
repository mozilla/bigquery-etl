-- Generated via bigquery_etl.glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.accounts_frontend.events_stream`
AS
SELECT
  *,
  STRUCT(
    STRUCT(
      LAX_BOOL(event_extra.cg) AS `cg`,
      LAX_BOOL(event_extra.cpu_arm) AS `cpu_arm`,
      LAX_BOOL(event_extra.hyb) AS `hyb`,
      LAX_BOOL(event_extra.ppa) AS `ppa`,
      LAX_BOOL(event_extra.prf) AS `prf`,
      LAX_BOOL(event_extra.rel) AS `rel`,
      LAX_BOOL(event_extra.supported) AS `supported`,
      LAX_BOOL(event_extra.third_party_links) AS `third_party_links`,
      LAX_BOOL(event_extra.uvpa) AS `uvpa`
    ) AS `boolean`,
    STRUCT(
      JSON_VALUE(event_extra.browser_family) AS `browser_family`,
      JSON_VALUE(event_extra.browser_major) AS `browser_major`,
      JSON_VALUE(event_extra.cg) AS `cg`,
      JSON_VALUE(event_extra.choice) AS `choice`,
      JSON_VALUE(event_extra.error_reason) AS `error_reason`,
      JSON_VALUE(event_extra.hyb) AS `hyb`,
      JSON_VALUE(event_extra.id) AS `id`,
      JSON_VALUE(event_extra.label) AS `label`,
      JSON_VALUE(event_extra.nimbus_user_id) AS `nimbus_user_id`,
      JSON_VALUE(event_extra.os_family) AS `os_family`,
      JSON_VALUE(event_extra.os_major) AS `os_major`,
      JSON_VALUE(event_extra.ppa) AS `ppa`,
      JSON_VALUE(event_extra.prf) AS `prf`,
      JSON_VALUE(event_extra.reason) AS `reason`,
      JSON_VALUE(event_extra.referrer) AS `referrer`,
      JSON_VALUE(event_extra.rel) AS `rel`,
      JSON_VALUE(event_extra.supported) AS `supported`,
      JSON_VALUE(event_extra.title) AS `title`,
      JSON_VALUE(event_extra.type) AS `type`,
      JSON_VALUE(event_extra.url) AS `url`,
      JSON_VALUE(event_extra.uvpa) AS `uvpa`
    ) AS `string`
  ) AS extras
FROM
  `moz-fx-data-shared-prod.accounts_frontend_derived.events_stream_v1`
