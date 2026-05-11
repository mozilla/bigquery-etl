WITH events_unnested AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    submission_timestamp,
    client_info.client_id AS client_id,
    SAFE_CAST(
      mozfun.norm.browser_version_info(client_info.app_display_version).major_version AS INT64
    ) AS app_version,
    normalized_os AS os,
    normalized_os_version AS os_version,
    client_info.windows_build_number AS windows_build_number,
    normalized_channel AS channel,
    client_info.locale AS locale,
    normalized_country_code AS country,
    sample_id,
    metrics.string.newtab_homepage_category AS homepage_category,
    metrics.string.newtab_newtab_category AS newtab_category,
    metrics.boolean.pocket_enabled AS organic_content_enabled,
    metrics.boolean.pocket_sponsored_stories_enabled AS sponsored_content_enabled,
    metrics.boolean.topsites_sponsored_enabled AS sponsored_topsites_enabled,
    metrics.boolean.topsites_enabled AS organic_topsites_enabled,
    metrics.boolean.newtab_search_enabled AS newtab_search_enabled,
    metrics.boolean.newtab_weather_enabled AS newtab_weather_enabled,
    metrics.uuid.legacy_telemetry_client_id AS legacy_telemetry_client_id,
    metrics.uuid.legacy_telemetry_profile_group_id AS profile_group_id,
    metadata.geo.subdivision1 AS geo_subdivision,
    metrics.string.search_engine_default_engine_id AS default_search_engine,
    metrics.string.search_engine_private_engine_id AS default_private_search_engine,
    metrics.quantity.topsites_rows AS topsite_rows,
    metrics.quantity.topsites_sponsored_tiles_configured AS topsite_sponsored_tiles_configured,
    metrics.string_list.newtab_blocked_sponsors AS newtab_blocked_sponsors,
    metrics.string.newtab_locale AS newtab_locale,
    metrics.string.newtab_content_surface_id AS newtab_content_surface_id,
    ping_info AS ping_info,
    mozfun.newtab.is_default_ui_v1(
      category,
      name,
      extra,
      metrics.string.newtab_homepage_category,
      metrics.string.newtab_newtab_category
    ) AS is_default_ui,
    category AS event_category,
    name AS event_name,
    extra AS event_details,
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_stable.newtab_v1`,
    UNNEST(events)
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND category = 'newtab'
    -- include `opened` so that is_default_ui can be derived per visit
    AND name IN ('opened', 'widgets_impression', 'widgets_user_event')
),
visit_aggregations AS (
  SELECT
    submission_date,
    client_id,
    mozfun.map.get_key(event_details, 'newtab_visit_id') AS newtab_visit_id,
      -- earliest ping submission_timestamp contributing to this visit
    MIN(submission_timestamp) AS submission_timestamp,
      -- ANY_VALUE: visit-level dimensions are consistent across events in a visit
    ANY_VALUE(app_version) AS app_version,
    ANY_VALUE(os) AS os,
    ANY_VALUE(os_version) AS os_version,
    ANY_VALUE(windows_build_number) AS windows_build_number,
    ANY_VALUE(channel) AS channel,
    ANY_VALUE(locale) AS locale,
    ANY_VALUE(country) AS country,
    ANY_VALUE(sample_id) AS sample_id,
    ANY_VALUE(legacy_telemetry_client_id) AS legacy_telemetry_client_id,
    ANY_VALUE(profile_group_id) AS profile_group_id,
    ANY_VALUE(geo_subdivision) AS geo_subdivision,
    ANY_VALUE(homepage_category) AS homepage_category,
    ANY_VALUE(newtab_category) AS newtab_category,
    ANY_VALUE(organic_content_enabled) AS organic_content_enabled,
    ANY_VALUE(sponsored_content_enabled) AS sponsored_content_enabled,
    ANY_VALUE(sponsored_topsites_enabled) AS sponsored_topsites_enabled,
    ANY_VALUE(organic_topsites_enabled) AS organic_topsites_enabled,
    ANY_VALUE(newtab_search_enabled) AS newtab_search_enabled,
    ANY_VALUE(newtab_weather_enabled) AS newtab_weather_enabled,
    ANY_VALUE(default_search_engine) AS default_search_engine,
    ANY_VALUE(default_private_search_engine) AS default_private_search_engine,
    ANY_VALUE(topsite_rows) AS topsite_rows,
    ANY_VALUE(topsite_sponsored_tiles_configured) AS topsite_sponsored_tiles_configured,
    ANY_VALUE(newtab_blocked_sponsors) AS newtab_blocked_sponsors,
    IFNULL(
      ANY_VALUE(newtab_content_surface_id),
      mozfun.newtab.scheduled_surface_id_v1(ANY_VALUE(country), ANY_VALUE(newtab_locale))
    ) AS newtab_content_surface_id,
    ANY_VALUE(ping_info.experiments) AS experiments,
    LOGICAL_OR(is_default_ui) AS is_default_ui,
  FROM
    events_unnested
  GROUP BY
    submission_date,
    client_id,
    newtab_visit_id
),
widget_metrics AS (
  SELECT
    submission_date,
    client_id,
    mozfun.map.get_key(event_details, 'newtab_visit_id') AS newtab_visit_id,
    mozfun.map.get_key(event_details, 'widget_name') AS widget_name,
    mozfun.map.get_key(event_details, 'widget_size') AS widget_size,
    COUNTIF(event_name = 'widgets_impression') AS impression_count,
    COUNTIF(event_name = 'widgets_user_event') AS user_event_count,
    COUNTIF(
      event_name = 'widgets_user_event'
      AND mozfun.map.get_key(event_details, 'user_action') IN ('change_size', 'learn_more')
    ) AS change_size_or_learn_more_count,
  FROM
    events_unnested
  WHERE
    event_name IN ('widgets_impression', 'widgets_user_event')
  GROUP BY
    submission_date,
    client_id,
    newtab_visit_id,
    widget_name,
    widget_size
),
user_action_counts_per_widget AS (
  SELECT
    submission_date,
    client_id,
    mozfun.map.get_key(event_details, 'newtab_visit_id') AS newtab_visit_id,
    mozfun.map.get_key(event_details, 'widget_name') AS widget_name,
    mozfun.map.get_key(event_details, 'widget_size') AS widget_size,
    mozfun.map.get_key(event_details, 'user_action') AS user_action,
    COUNT(*) AS count,
  FROM
    events_unnested
  WHERE
    event_name = 'widgets_user_event'
  GROUP BY
    submission_date,
    client_id,
    newtab_visit_id,
    widget_name,
    widget_size,
    user_action
),
user_action_counts_summary AS (
  SELECT
    submission_date,
    client_id,
    newtab_visit_id,
    widget_name,
    widget_size,
    ARRAY_AGG(STRUCT(user_action, count)) AS user_action_counts,
  FROM
    user_action_counts_per_widget
  GROUP BY
    submission_date,
    client_id,
    newtab_visit_id,
    widget_name,
    widget_size
)
SELECT
  widget_metrics.*,
  visit_aggregations.* EXCEPT (submission_date, client_id, newtab_visit_id),
  user_action_counts_summary.user_action_counts,
FROM
  widget_metrics
LEFT JOIN
  visit_aggregations
  USING (submission_date, client_id, newtab_visit_id)
LEFT JOIN
  user_action_counts_summary
  USING (submission_date, client_id, newtab_visit_id, widget_name, widget_size)
