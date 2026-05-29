WITH user_action_counts_per_widget AS (
    -- unnest each visit's user_action_counts and sum per user_action to client-day grain
  SELECT
    submission_date,
    client_id,
    widget_name,
    widget_size,
    uac.user_action,
    SUM(uac.count) AS count,
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_derived.widgets_visit_daily_v1`,
    UNNEST(user_action_counts) AS uac
  WHERE
    submission_date = @submission_date
  GROUP BY
    submission_date,
    client_id,
    widget_name,
    widget_size,
    uac.user_action
),
user_action_counts_summary AS (
  SELECT
    submission_date,
    client_id,
    widget_name,
    widget_size,
    ARRAY_AGG(STRUCT(user_action, count)) AS user_action_counts,
  FROM
    user_action_counts_per_widget
  GROUP BY
    submission_date,
    client_id,
    widget_name,
    widget_size
),
widget_enabled_users AS (
  SELECT DISTINCT
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id AS client_id,
    widget AS widget_name,
    TRUE AS is_widget_enabled
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_stable.newtab_v1`,
    UNNEST(metrics.string_list.newtab_widgets_enabled_list) AS widget
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND NULLIF(widget, '') IS NOT NULL
),
client_metrics AS (
  SELECT
    submission_date,
    client_id,
    widget_name,
    widget_size,
    LOGICAL_OR(
      COALESCE(is_widget_enabled, FALSE)
      OR COALESCE(impression_count > 0, FALSE)
    ) AS is_widget_enabled,
      -- mode_last: pick the most frequent occurring value across visits
    `moz-fx-data-shared-prod.udf.mode_last`(ARRAY_AGG(app_version)) AS app_version,
    `moz-fx-data-shared-prod.udf.mode_last`(ARRAY_AGG(os)) AS os,
    `moz-fx-data-shared-prod.udf.mode_last`(ARRAY_AGG(os_version)) AS os_version,
    `moz-fx-data-shared-prod.udf.mode_last`(
      ARRAY_AGG(windows_build_number)
    ) AS windows_build_number,
    `moz-fx-data-shared-prod.udf.mode_last`(ARRAY_AGG(channel)) AS channel,
    `moz-fx-data-shared-prod.udf.mode_last`(ARRAY_AGG(locale)) AS locale,
    `moz-fx-data-shared-prod.udf.mode_last`(ARRAY_AGG(country)) AS country,
    `moz-fx-data-shared-prod.udf.mode_last`(ARRAY_AGG(geo_subdivision)) AS geo_subdivision,
    `moz-fx-data-shared-prod.udf.mode_last`(ARRAY_AGG(homepage_category)) AS homepage_category,
    `moz-fx-data-shared-prod.udf.mode_last`(ARRAY_AGG(newtab_category)) AS newtab_category,
    `moz-fx-data-shared-prod.udf.mode_last`(
      ARRAY_AGG(organic_content_enabled)
    ) AS organic_content_enabled,
    `moz-fx-data-shared-prod.udf.mode_last`(
      ARRAY_AGG(sponsored_content_enabled)
    ) AS sponsored_content_enabled,
    `moz-fx-data-shared-prod.udf.mode_last`(
      ARRAY_AGG(sponsored_topsites_enabled)
    ) AS sponsored_topsites_enabled,
    `moz-fx-data-shared-prod.udf.mode_last`(
      ARRAY_AGG(organic_topsites_enabled)
    ) AS organic_topsites_enabled,
    `moz-fx-data-shared-prod.udf.mode_last`(
      ARRAY_AGG(newtab_search_enabled)
    ) AS newtab_search_enabled,
    `moz-fx-data-shared-prod.udf.mode_last`(
      ARRAY_AGG(newtab_weather_enabled)
    ) AS newtab_weather_enabled,
    `moz-fx-data-shared-prod.udf.mode_last`(
      ARRAY_AGG(default_search_engine)
    ) AS default_search_engine,
    `moz-fx-data-shared-prod.udf.mode_last`(
      ARRAY_AGG(default_private_search_engine)
    ) AS default_private_search_engine,
    `moz-fx-data-shared-prod.udf.mode_last`(ARRAY_AGG(topsite_rows)) AS topsite_rows,
    `moz-fx-data-shared-prod.udf.mode_last`(
      ARRAY_AGG(topsite_sponsored_tiles_configured)
    ) AS topsite_sponsored_tiles_configured,
    `moz-fx-data-shared-prod.udf.mode_last`(
      ARRAY_AGG(newtab_content_surface_id)
    ) AS newtab_content_surface_id,
    ANY_VALUE(sample_id) AS sample_id,
    ANY_VALUE(legacy_telemetry_client_id) AS legacy_telemetry_client_id,
    ANY_VALUE(profile_group_id) AS profile_group_id,
    ANY_VALUE(experiments) AS experiments,
    ANY_VALUE(newtab_blocked_sponsors) AS newtab_blocked_sponsors,
      -- visit-level counts: number of distinct visits where this (widget_name, widget_size)
      -- had any qualifying event
    COUNT(DISTINCT newtab_visit_id) AS all_visits,
    COUNT(DISTINCT IF(is_default_ui, newtab_visit_id, NULL)) AS default_ui_visits,
    COUNT(DISTINCT IF(impression_count > 0, newtab_visit_id, NULL)) AS widget_impression_visits,
    COUNT(DISTINCT IF(user_event_count > 0, newtab_visit_id, NULL)) AS widget_user_event_visits,
    COUNT(DISTINCT IF(enabled_count > 0, newtab_visit_id, NULL)) AS widget_enabled_visits,
    COUNT(DISTINCT IF(disabled_count > 0, newtab_visit_id, NULL)) AS widget_disabled_visits,
      -- summed event counts across visits
    SUM(impression_count) AS impression_count,
    SUM(user_event_count) AS user_event_count,
    SUM(change_size_or_learn_more_count) AS change_size_or_learn_more_count,
    SUM(enabled_count) AS enabled_count,
    SUM(disabled_count) AS disabled_count,
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_derived.widgets_visit_daily_v1`
  FULL OUTER JOIN
    widget_enabled_users
    USING (submission_date, client_id, widget_name)
  WHERE
    submission_date = @submission_date
  GROUP BY
    submission_date,
    client_id,
    widget_name,
    widget_size
)
SELECT
  client_metrics.*,
  user_action_counts_summary.user_action_counts,
FROM
  client_metrics
LEFT JOIN
  user_action_counts_summary
  USING (submission_date, client_id, widget_name, widget_size)
