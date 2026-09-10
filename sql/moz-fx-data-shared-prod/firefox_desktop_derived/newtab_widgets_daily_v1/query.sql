WITH widget_events AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
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
    -- DENG-11596: the crossword widget emitted a `widgets_user_event` on every
    -- keystroke and cursor move, plus echoes of context menu actions. These are not
    -- genuine user actions, so they are excluded from all interaction counts.
    -- Expressed as an allowlist of known-bad action values so that newly introduced
    -- action values default to being counted. See DENG-11450 for the criteria.
    AND NOT (
      event.category = 'newtab'
      AND event.name = 'widgets_user_event'
      AND mozfun.map.get_key(event.extra, 'widget_name') = 'crossword'
      AND mozfun.map.get_key(event.extra, 'user_action') = 'interaction'
      AND mozfun.map.get_key(event.extra, 'action_value') IN (
        -- keystroke and cursor noise
        'input_letter',
        'cell_click',
        'backspace',
        'next_clue_button',
        'previous_clue_button',
        'review_clue_selected',
        -- context menu echoes
        'all_clues_opened',
        'all_clues_closed',
        'reveal_grid_requested',
        'reveal_grid_completed'
      )
    )
),
aggregated AS (
  SELECT
    submission_date,
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
        'change_location',
        'change_temperature_units',
        'change_weather_display',
        'detect_location'
      )
    ) AS widget_setting_change_count,
    COUNTIF(
      event_name = 'widgets_user_event'
      AND user_action IN (
        'list_copy',
        'list_create',
        'list_delete',
        'list_edit',
        'task_complete',
        'task_create',
        'task_delete',
        'task_edit',
        'timer_end',
        'timer_pause',
        'timer_play',
        'timer_set',
        'timer_toggle_focus',
        'timer_toggle_play'
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
