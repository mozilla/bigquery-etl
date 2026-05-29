WITH widget_events AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    SAFE_CAST(
      mozfun.norm.browser_version_info(client_info.app_display_version).major_version AS INT64
    ) AS app_version,
    normalized_os AS os,
    normalized_channel AS channel,
    client_info.locale AS locale,
    normalized_country_code AS country,
    client_info.client_id AS client_id,
    event.name AS event_name,
    mozfun.map.get_key(event.extra, 'widget_name') AS widget_name,
    SAFE_CAST(mozfun.map.get_key(event.extra, 'enabled') AS BOOL) AS widget_enabled,
    mozfun.map.get_key(event.extra, 'user_action') AS user_action,
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_stable.newtab_v1`,
    UNNEST(events) AS event
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND event.category = 'newtab'
    AND event.name IN ('widgets_impression', 'widgets_user_event', 'widgets_enabled')
    AND mozfun.map.get_key(event.extra, 'widget_name') IS NOT NULL
),
aggregated AS (
  SELECT
    submission_date,
    app_version,
    os,
    channel,
    locale,
    country,
    widget_name,
    COUNT(
      DISTINCT IF(event_name IN ('widgets_user_event', 'widgets_enabled'), client_id, NULL)
    ) AS widget_engaged_clients,
    COUNTIF(event_name = 'widgets_enabled' AND widget_enabled) AS widget_enabled_count,
    COUNTIF(event_name = 'widgets_enabled' AND NOT widget_enabled) AS widget_disabled_count,
    COUNTIF(event_name = 'widgets_impression') AS widget_impression_count,
    COUNTIF(event_name = 'widgets_user_event') AS widget_user_event_count,
    COUNTIF(
      event_name = 'widgets_user_event'
      AND user_action IN ('learn_more', 'provider_link_click')
    ) AS widget_link_click_count,
    COUNTIF(
      event_name = 'widgets_user_event'
      AND user_action IN (
        'change_hour_format',
        'change_location',
        'change_size',
        'change_temperature_units',
        'change_weather_display',
        'collapse',
        'detect_location',
        'expand'
      )
    ) AS widget_setting_change_count,
    COUNTIF(
      event_name = 'widgets_user_event'
      AND user_action IN (
        'add_clock',
        'add_nickname',
        'edit_clock',
        'follow_teams',
        'list_copy',
        'list_create',
        'list_delete',
        'list_edit',
        'open_match_search',
        'remove_clock',
        'save_teams',
        'task_complete',
        'task_create',
        'task_delete',
        'task_edit',
        'timer_end',
        'timer_pause',
        'timer_play',
        'timer_reset',
        'timer_set',
        'timer_toggle_break',
        'timer_toggle_focus',
        'timer_toggle_play',
        'view_key_dates',
        'view_schedule',
        'view_upcoming'
      )
    ) AS widget_utility_action_count,
    COUNTIF(
      event_name = 'widgets_user_event'
      AND user_action = 'opt_in_accepted'
    ) AS widget_optin_accept_count,
  FROM
    widget_events
  GROUP BY
    submission_date,
    app_version,
    os,
    channel,
    locale,
    country,
    widget_name
),
user_action_array AS (
  SELECT
    submission_date,
    app_version,
    os,
    channel,
    locale,
    country,
    widget_name,
    ARRAY_AGG(
      STRUCT(user_action AS action, action_count AS count)
      ORDER BY
        user_action
    ) AS widget_user_action_counts,
  FROM
    (
      SELECT
        submission_date,
        app_version,
        os,
        channel,
        locale,
        country,
        widget_name,
        user_action,
        COUNT(*) AS action_count,
      FROM
        widget_events
      WHERE
        event_name = 'widgets_user_event'
        AND user_action IS NOT NULL
      GROUP BY
        submission_date,
        app_version,
        os,
        channel,
        locale,
        country,
        widget_name,
        user_action
    )
  GROUP BY
    submission_date,
    app_version,
    os,
    channel,
    locale,
    country,
    widget_name
),
enabled_users_aggregate AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    SAFE_CAST(
      mozfun.norm.browser_version_info(client_info.app_display_version).major_version AS INT64
    ) AS app_version,
    normalized_os AS os,
    normalized_channel AS channel,
    client_info.locale AS locale,
    normalized_country_code AS country,
    widget AS widget_name,
    COUNT(DISTINCT client_info.client_id) AS widget_enabled_clients
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_stable.newtab_v1`,
    UNNEST(metrics.string_list.newtab_widgets_enabled_list) AS widget
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND NULLIF(widget, '') IS NOT NULL
  GROUP BY
    submission_date,
    app_version,
    os,
    channel,
    locale,
    country,
    widget_name
)
SELECT
  submission_date,
  widget_name,
  eua.widget_enabled_clients,
  agg.widget_engaged_clients,
  agg.widget_enabled_count,
  agg.widget_disabled_count,
  agg.widget_impression_count,
  agg.widget_user_event_count,
  agg.widget_link_click_count,
  agg.widget_setting_change_count,
  agg.widget_utility_action_count,
  agg.widget_optin_accept_count,
  uaa.widget_user_action_counts,
FROM
  aggregated AS agg
LEFT JOIN
  user_action_array AS uaa
  USING (submission_date, widget_name, app_version, os, channel, locale, country)
LEFT JOIN
  enabled_users_aggregate AS eua
  USING (submission_date, widget_name, app_version, os, channel, locale, country)
