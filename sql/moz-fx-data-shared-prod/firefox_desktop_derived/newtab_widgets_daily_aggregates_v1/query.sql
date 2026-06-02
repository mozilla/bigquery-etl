WITH action_counts_per_widget AS (
    -- unnest each visit's user_action_counts and sum per user_action to widget-day grain
  SELECT
    submission_date,
    widget_name,
    app_version,
    os,
    channel,
    country,
    locale,
    uac.user_action,
    SUM(uac.count) AS action_count,
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_derived.widgets_client_daily_v1`,
    UNNEST(user_action_counts) AS uac
  WHERE
    submission_date = @submission_date
  GROUP BY
    submission_date,
    widget_name,
    app_version,
    os,
    channel,
    country,
    locale,
    uac.user_action
),
action_counts_summary AS (
  SELECT
    submission_date,
    widget_name,
    app_version,
    os,
    channel,
    country,
    locale,
    SUM(
      IF(user_action IN ('learn_more', 'provider_link_click'), action_count, 0)
    ) AS widget_link_click_count,
    SUM(
      IF(
        user_action IN (
          'change_hour_format',
          'change_location',
          'change_size',
          'change_temperature_units',
          'change_weather_display',
          'collapse',
          'detect_location',
          'expand'
        ),
        action_count,
        0
      )
    ) AS widget_setting_change_count,
    SUM(
      IF(
        user_action IN (
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
        ),
        action_count,
        0
      )
    ) AS widget_utility_action_count,
    SUM(IF(user_action = 'opt_in_accepted', action_count, 0)) AS widget_optin_accept_count,
    ARRAY_AGG(STRUCT(user_action, action_count)) AS widget_user_action_counts
  FROM
    action_counts_per_widget
  GROUP BY
    submission_date,
    widget_name,
    app_version,
    os,
    channel,
    country,
    locale
),
widget_metrics AS (
  SELECT
    submission_date,
    widget_name,
    app_version,
    os,
    channel,
    country,
    locale,
    COUNT(DISTINCT IF(is_widget_enabled, client_id, NULL)) AS widget_enabled_clients,
    COUNT(
      DISTINCT IF(is_widget_enabled AND all_visits > 0, client_id, NULL)
    ) AS widget_enabled_clients_with_visit,
    COUNT(
      DISTINCT IF(user_event_count + enabled_count > 0, client_id, NULL)
    ) AS widget_engaged_clients,
    COUNT(DISTINCT IF(default_ui_visits > 0, client_id, NULL)) AS default_ui_clients,
    SUM(enabled_count) AS widget_enabled_count,
    SUM(disabled_count) AS widget_disabled_count,
    SUM(impression_count) AS widget_impression_count,
    SUM(user_event_count) AS widget_user_event_count,
    SUM(all_visits) AS all_visits,
    SUM(default_ui_visits) AS default_ui_visits,
    SUM(widget_impression_visits) AS widget_impression_visits,
    SUM(widget_user_event_visits) AS widget_user_event_visits,
    SUM(widget_enabled_visits) AS widget_enabled_visits,
    SUM(widget_disabled_visits) AS widget_disabled_visits,
    SUM(change_size_or_learn_more_count) AS change_size_or_learn_more_count
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_derived.widgets_client_daily_v1`
  WHERE
    submission_date = @submission_date
  GROUP BY
    submission_date,
    widget_name,
    app_version,
    os,
    channel,
    country,
    locale
)
SELECT
  widget_metrics.*,
  action_counts_summary.widget_link_click_count,
  action_counts_summary.widget_setting_change_count,
  action_counts_summary.widget_utility_action_count,
  action_counts_summary.widget_optin_accept_count,
  action_counts_summary.widget_user_action_counts
FROM
  widget_metrics
LEFT JOIN
  action_counts_summary
  USING (submission_date, widget_name, app_version, os, channel, country, locale)
