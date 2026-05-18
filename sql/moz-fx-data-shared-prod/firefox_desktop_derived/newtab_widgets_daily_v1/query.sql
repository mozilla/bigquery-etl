WITH widget_events AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id AS client_id,
    event.name AS event_name,
    mozfun.map.get_key(event.extra, 'widget_name') AS widget_name,
    mozfun.map.get_key(event.extra, 'enabled') AS enabled,
    mozfun.map.get_key(event.extra, 'user_action') AS user_action,
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_stable.newtab_v1`,
    UNNEST(events) AS event
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND event.name IN ('widgets_impression', 'widgets_user_event', 'widgets_enabled')
    AND mozfun.map.get_key(event.extra, 'widget_name') IS NOT NULL
),
aggregated AS (
  SELECT
    submission_date,
    widget_name,
    COUNT(
      DISTINCT IF(event_name IN ('widgets_user_event', 'widgets_enabled'), client_id, NULL)
    ) AS widget_engaged_clients,
    COUNTIF(event_name = 'widgets_enabled' AND enabled = 'true') AS widget_enabled_count,
    COUNTIF(event_name = 'widgets_enabled' AND enabled = 'false') AS widget_disabled_count,
    COUNTIF(event_name = 'widgets_impression') AS widget_impression_count,
    COUNTIF(event_name = 'widgets_user_event') AS widget_user_event_count,
    COUNTIF(
      event_name = 'widgets_user_event'
      AND user_action IN ('provider_link_click', 'learn_more')
    ) AS widget_link_click_count,
    COUNTIF(
      event_name = 'widgets_user_event'
      AND user_action IN (
        'change_location',
        'change_temperature_units',
        'change_weather_display',
        'detect_location'
      )
    ) AS widget_setting_change_count,
    COUNTIF(
      event_name = 'widgets_user_event'
      AND user_action IN (
        'timer_set',
        'timer_play',
        'timer_end',
        'timer_pause',
        'timer_toggle_play',
        'timer_toggle_focus',
        'list_create',
        'list_edit',
        'list_copy',
        'list_delete',
        'task_complete',
        'task_create',
        'task_delete',
        'task_edit'
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
    widget_name
),
user_action_array AS (
  SELECT
    submission_date,
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
        widget_name,
        user_action
    )
  GROUP BY
    submission_date,
    widget_name
)
SELECT
  agg.submission_date,
  agg.widget_name,
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
  USING (submission_date, widget_name)
