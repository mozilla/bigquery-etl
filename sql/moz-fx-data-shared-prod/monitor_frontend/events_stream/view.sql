-- Generated via bigquery_etl.glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.monitor_frontend.events_stream`
AS
SELECT
  *,
  STRUCT(
    STRUCT(LAX_BOOL(event_extra.legacy_user) AS `legacy_user`) AS `boolean`,
    STRUCT(
      LAX_INT64(event_extra.breach_count) AS `breach_count`,
      LAX_INT64(event_extra.broker_count) AS `broker_count`,
      LAX_INT64(event_extra.legacy_breach_count) AS `legacy_breach_count`
    ) AS `quantity`,
    STRUCT(
      JSON_VALUE(event_extra.automated_removal_period) AS `automated_removal_period`,
      JSON_VALUE(event_extra.banner_id) AS `banner_id`,
      JSON_VALUE(event_extra.button_id) AS `button_id`,
      JSON_VALUE(event_extra.dashboard_tab) AS `dashboard_tab`,
      JSON_VALUE(event_extra.experiment_branch) AS `experiment_branch`,
      JSON_VALUE(event_extra.field_id) AS `field_id`,
      JSON_VALUE(event_extra.flow_id) AS `flow_id`,
      JSON_VALUE(event_extra.id) AS `id`,
      JSON_VALUE(event_extra.label) AS `label`,
      JSON_VALUE(event_extra.last_scan_date) AS `last_scan_date`,
      JSON_VALUE(event_extra.link_id) AS `link_id`,
      JSON_VALUE(event_extra.nimbus_app_id) AS `nimbus_app_id`,
      JSON_VALUE(event_extra.nimbus_branch) AS `nimbus_branch`,
      JSON_VALUE(event_extra.nimbus_experiment) AS `nimbus_experiment`,
      JSON_VALUE(event_extra.nimbus_experiment_type) AS `nimbus_experiment_type`,
      JSON_VALUE(event_extra.nimbus_is_preview) AS `nimbus_is_preview`,
      JSON_VALUE(event_extra.nimbus_user_id) AS `nimbus_user_id`,
      JSON_VALUE(event_extra.path) AS `path`,
      JSON_VALUE(event_extra.plan_tier) AS `plan_tier`,
      JSON_VALUE(event_extra.popup_id) AS `popup_id`,
      JSON_VALUE(event_extra.referrer) AS `referrer`,
      JSON_VALUE(event_extra.response_id) AS `response_id`,
      JSON_VALUE(event_extra.session_id) AS `session_id`,
      JSON_VALUE(event_extra.survey_id) AS `survey_id`,
      JSON_VALUE(event_extra.title) AS `title`,
      JSON_VALUE(event_extra.type) AS `type`,
      JSON_VALUE(event_extra.url) AS `url`,
      JSON_VALUE(event_extra.user_id) AS `user_id`,
      JSON_VALUE(event_extra.utm_campaign) AS `utm_campaign`,
      JSON_VALUE(event_extra.utm_content) AS `utm_content`,
      JSON_VALUE(event_extra.utm_medium) AS `utm_medium`,
      JSON_VALUE(event_extra.utm_source) AS `utm_source`,
      JSON_VALUE(event_extra.utm_term) AS `utm_term`
    ) AS `string`
  ) AS extras
FROM
  `moz-fx-data-shared-prod.monitor_frontend_derived.events_stream_v1`
