-- Ticket-level SLA snapshot for Zendesk support tickets.
-- One row per ticket with first reply, full resolution, FCR, and CSAT metrics.
-- Business minutes computed against "Mozilla Support Hours" with holiday subtraction,
-- mirroring the calculation Zendesk surfaces in its UI.
--
-- Rebuilt over a rolling 13-month window so late-arriving updates
-- (full_resolution status flips, CSAT survey responses, reopens) flow through.
WITH tickets_in_window AS (
  SELECT
    t.id AS ticket_id,
    t.created_at AS ticket_created_at,
    DATE(t.created_at) AS ticket_created_date,
    t.status,
    t.group_id,
    t.custom_product,
    COALESCE(m.product_mapping, t.custom_product) AS product
  FROM
    `moz-fx-data-shared-prod.zendesk_syndicate.ticket` AS t
  LEFT JOIN
    `moz-fx-data-shared-prod.static.cx_product_mappings_v1` AS m
    ON m.product = t.custom_product
    AND m.source = 'Zendesk'
  WHERE
    DATE(t.created_at)
    BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 13 MONTH)
    AND CURRENT_DATE()
    AND t.status != 'deleted'
),
-- Public agent/admin replies, scoped to in-window tickets.
ticket_comments_with_role AS (
  SELECT
    tc.ticket_id,
    tc.created AS comment_created_at,
    u.role AS author_role
  FROM
    `moz-fx-sumo-prod.zendesk.ticket_comment` AS tc
  JOIN
    `moz-fx-sumo-prod.zendesk.user` AS u
    ON tc.user_id = u.id
  JOIN
    tickets_in_window AS tw
    ON tw.ticket_id = tc.ticket_id
  WHERE
    tc.public = TRUE
    AND u.role IN ('agent', 'admin')
),
first_agent_reply AS (
  SELECT
    ticket_id,
    MIN(comment_created_at) AS first_reply_at
  FROM
    ticket_comments_with_role
  GROUP BY
    ticket_id
),
agent_reply_counts AS (
  SELECT
    ticket_id,
    COUNT(*) AS agent_reply_count
  FROM
    ticket_comments_with_role
  GROUP BY
    ticket_id
),
-- Status history scoped to in-window tickets, used for solve/close timestamps and reopen counts.
status_history AS (
  SELECT
    h.ticket_id,
    h.value AS status_value,
    h.updated AS status_updated_at
  FROM
    `moz-fx-sumo-prod.zendesk.ticket_field_history` AS h
  JOIN
    tickets_in_window AS tw
    ON tw.ticket_id = h.ticket_id
  WHERE
    h.field_name = 'status'
),
-- Full resolution = last 'solved' timestamp; falls back to last 'closed' for tickets that
-- went straight from open → closed without a 'solved' event.
ticket_full_solved AS (
  SELECT
    ticket_id,
    COALESCE(
      MAX(CASE WHEN status_value = 'solved' THEN status_updated_at END),
      MAX(CASE WHEN status_value = 'closed' THEN status_updated_at END)
    ) AS full_resolution_at,
    MAX(CASE WHEN status_value = 'solved' THEN status_updated_at END) AS solved_at,
    MAX(CASE WHEN status_value = 'closed' THEN status_updated_at END) AS closed_at
  FROM
    status_history
  GROUP BY
    ticket_id
),
-- A reopen is a 'solved' → 'open' transition.
reopens AS (
  SELECT
    ticket_id,
    COUNTIF(prev_status = 'solved' AND status_value = 'open') AS reopen_count
  FROM
    (
      SELECT
        ticket_id,
        status_value,
        LAG(status_value, 1, 'new') OVER (
          PARTITION BY
            ticket_id
          ORDER BY
            status_updated_at
        ) AS prev_status
      FROM
        status_history
    )
  GROUP BY
    ticket_id
),
-- Active schedule for business-hours math. ticket_schedule (Enterprise-only) isn't in
-- this dataset, so a single schedule applies to all tickets.
-- Map Zendesk's friendly time-zone label to a DST-aware IANA zone for DATETIME().
active_schedule AS (
  SELECT
    id AS schedule_id,
    ANY_VALUE(
      CASE
        time_zone
        WHEN 'Central Time (US & Canada)'
          THEN 'America/Chicago'
        WHEN 'Eastern Time (US & Canada)'
          THEN 'America/New_York'
        WHEN 'Mountain Time (US & Canada)'
          THEN 'America/Denver'
        WHEN 'Pacific Time (US & Canada)'
          THEN 'America/Los_Angeles'
        ELSE time_zone
      END
    ) AS schedule_tz
  FROM
    `moz-fx-sumo-prod.zendesk.schedule`
  WHERE
    name = 'Mozilla Support Hours'
  GROUP BY
    id
),
-- Unify first_reply and full_resolution into one event stream so the schedule/holiday
-- pipeline runs once per (ticket × event) instead of being duplicated per metric.
ticket_events AS (
  SELECT
    tw.ticket_id,
    tw.ticket_created_at,
    far.first_reply_at AS event_at,
    'first_reply' AS event_type
  FROM
    tickets_in_window AS tw
  JOIN
    first_agent_reply AS far
    ON tw.ticket_id = far.ticket_id
  UNION ALL
  SELECT
    tw.ticket_id,
    tw.ticket_created_at,
    tfs.full_resolution_at,
    'full_resolution'
  FROM
    tickets_in_window AS tw
  JOIN
    ticket_full_solved AS tfs
    ON tw.ticket_id = tfs.ticket_id
  WHERE
    tfs.full_resolution_at IS NOT NULL
),
-- Convert ticket times into the schedule's local time zone. schedule.start_time/end_time
-- are minutes-from-Sunday-midnight in the schedule's own TZ, not UTC.
ticket_event_window AS (
  SELECT
    te.ticket_id,
    te.event_type,
    te.ticket_created_at,
    te.event_at,
    a.schedule_id,
    a.schedule_tz,
    ROUND(TIMESTAMP_DIFF(te.event_at, te.ticket_created_at, SECOND) / 60.0, 1) AS calendar_minutes,
    DATETIME_DIFF(
      DATETIME(te.ticket_created_at, a.schedule_tz),
      DATETIME_TRUNC(DATETIME(te.ticket_created_at, a.schedule_tz), WEEK),
      SECOND
    ) / 60.0 AS start_time_in_minutes_from_week,
    -- GREATEST guards against any data weirdness where event_at < created_at.
    GREATEST(
      0,
      ROUND(TIMESTAMP_DIFF(te.event_at, te.ticket_created_at, SECOND) / 60.0, 0)
    ) AS raw_delta_in_minutes
  FROM
    ticket_events AS te
  CROSS JOIN
    active_schedule AS a
),
-- Expand each (ticket × event) into one row per calendar week the event spans.
weekly_periods AS (
  SELECT
    ticket_id,
    event_type,
    ticket_created_at,
    event_at,
    schedule_id,
    schedule_tz,
    calendar_minutes,
    start_time_in_minutes_from_week,
    raw_delta_in_minutes,
    week_number,
    GREATEST(
      0,
      start_time_in_minutes_from_week - week_number * (7 * 24 * 60)
    ) AS ticket_week_start_time,
    LEAST(
      start_time_in_minutes_from_week + raw_delta_in_minutes - week_number * (7 * 24 * 60),
      (7 * 24 * 60)
    ) AS ticket_week_end_time
  FROM
    ticket_event_window,
    UNNEST(
      GENERATE_ARRAY(
        0,
        CAST(
          FLOOR((start_time_in_minutes_from_week + raw_delta_in_minutes) / (7 * 24 * 60)) AS INT64
        ),
        1
      )
    ) AS week_number
),
-- Intersect each week's window with the schedule. The s.id = wp.schedule_id filter is
-- critical — without it every schedule in the table contributes, multiplying minutes.
intercepted_periods AS (
  SELECT
    wp.ticket_id,
    wp.event_type,
    wp.schedule_id,
    wp.schedule_tz,
    wp.week_number,
    wp.ticket_created_at,
    wp.event_at,
    wp.calendar_minutes,
    s.start_time AS schedule_interval_start,
    LEAST(wp.ticket_week_end_time, s.end_time) - GREATEST(
      wp.ticket_week_start_time,
      s.start_time
    ) AS scheduled_minutes
  FROM
    weekly_periods AS wp
  JOIN
    `moz-fx-sumo-prod.zendesk.schedule` AS s
    ON s.id = wp.schedule_id
    AND wp.ticket_week_start_time <= s.end_time
    AND wp.ticket_week_end_time >= s.start_time
),
-- Calendar date each scheduled interval falls on, used for holiday matching.
intercepted_with_dates AS (
  SELECT
    ip.*,
    DATE_ADD(
      DATE_TRUNC(DATE(DATETIME(ticket_created_at, schedule_tz)), WEEK),
      INTERVAL CAST(7 * week_number + FLOOR(schedule_interval_start / 1440) AS INT64) DAY
    ) AS schedule_interval_date
  FROM
    intercepted_periods AS ip
),
holiday_minutes_per_event AS (
  SELECT
    iwd.ticket_id,
    iwd.event_type,
    SUM(iwd.scheduled_minutes) AS holiday_minutes
  FROM
    intercepted_with_dates AS iwd
  JOIN
    `moz-fx-sumo-prod.zendesk.schedule_holiday` AS sh
    ON sh.schedule_id = iwd.schedule_id
    AND iwd.schedule_interval_date
    BETWEEN DATE(sh.start_date)
    AND DATE(sh.end_date)
  GROUP BY
    iwd.ticket_id,
    iwd.event_type
),
-- Drives off ticket_events (not intercepted_periods) so every event emits a row —
-- including auto-solved tickets whose interval is too short or entirely outside
-- business hours to overlap a schedule slot. Those produce business_minutes = 0
-- instead of dropping out, which keeps full_resolution_at populated downstream.
business_event_times AS (
  SELECT
    te.ticket_id,
    te.event_type,
    te.event_at,
    ROUND(TIMESTAMP_DIFF(te.event_at, te.ticket_created_at, SECOND) / 60.0, 1) AS calendar_minutes,
    ROUND(COALESCE(sched.business_minutes, 0), 1) AS business_minutes
  FROM
    ticket_events AS te
  LEFT JOIN
    (
      SELECT
        ip.ticket_id,
        ip.event_type,
        SUM(ip.scheduled_minutes) - COALESCE(MAX(hm.holiday_minutes), 0) AS business_minutes
      FROM
        intercepted_periods AS ip
      LEFT JOIN
        holiday_minutes_per_event AS hm
        ON ip.ticket_id = hm.ticket_id
        AND ip.event_type = hm.event_type
      GROUP BY
        ip.ticket_id,
        ip.event_type
    ) AS sched
    ON te.ticket_id = sched.ticket_id
    AND te.event_type = sched.event_type
),
-- Appbot/non-Appbot classification used to filter out non-English Appbot reviews.
appbot_class AS (
  SELECT
    tt.ticket_id,
    CASE
      WHEN LOGICAL_OR(tt.tag = 'appbot')
        AND LOGICAL_OR(tt.tag IN ('english', 'usa', 'unitedkingdom', 'canada', 'australia'))
        THEN 'Appbot - English'
      WHEN LOGICAL_OR(tt.tag = 'appbot')
        AND NOT LOGICAL_OR(tt.tag IN ('english', 'usa', 'unitedkingdom', 'canada', 'australia'))
        THEN 'Appbot - Non-English'
      ELSE 'All Other Tickets'
    END AS ticket_group
  FROM
    `moz-fx-sumo-prod.zendesk.ticket_tag` AS tt
  JOIN
    tickets_in_window AS tw
    ON tw.ticket_id = tt.ticket_id
  GROUP BY
    tt.ticket_id
),
-- Tag-based automation classification. Reopened automation tickets are reclassified
-- as human-handled — once a human had to step in, the resolution wasn't really automated.
automation_tags AS (
  SELECT
    tt.ticket_id,
    MAX(
      CASE
        WHEN tt.tag IN (
            'ssa-sign-in-failure-automation',
            'ssa-connection-issues-automation',
            'ssa-sync-data-automation',
            'appbot-autosolve',
            'ssa-experiment-2fa-automation',
            'ssa-experiment-pwrdreset-automation',
            'ssa-experiment-emailverify-automation',
            'ssa-experiment-4-star',
            'ssa-experiment-5-star',
            'loginless-autosolve'
          )
          THEN 1
        ELSE 0
      END
    ) AS is_automated
  FROM
    `moz-fx-sumo-prod.zendesk.ticket_tag` AS tt
  JOIN
    tickets_in_window AS tw
    ON tw.ticket_id = tt.ticket_id
  GROUP BY
    tt.ticket_id
),
test_tickets AS (
  SELECT DISTINCT
    tt.ticket_id
  FROM
    `moz-fx-sumo-prod.zendesk.ticket_tag` AS tt
  JOIN
    tickets_in_window AS tw
    ON tw.ticket_id = tt.ticket_id
  WHERE
    tt.tag LIKE '%test%'
),
csat AS (
  SELECT
    t_s.ticket_id,
    a_s.rating_category
  FROM
    `moz-fx-sumo-prod.zendesk.csat_survey_answer` AS a_s
  JOIN
    `moz-fx-sumo-prod.zendesk.ticket_csat_survey` AS t_s
    ON a_s.survey_response_id = t_s.survey_response_id
    AND a_s.type = 'rating_scale'
)
SELECT
  tw.ticket_id,
  tw.ticket_created_date,
  tw.ticket_created_at,
  tw.product,
  tw.custom_product AS raw_custom_product,
  COALESCE(ac.ticket_group, 'All Other Tickets') AS ticket_group,
  CASE
    WHEN COALESCE(aut.is_automated, 0) = 1
      AND COALESCE(r.reopen_count, 0) = 0
      THEN 'automation'
    ELSE 'human-handled'
  END AS automation_category,
  fr.event_at AS first_reply_at,
  fr.calendar_minutes AS first_reply_time_calendar_minutes,
  fr.business_minutes AS first_reply_time_business_minutes,
  res.event_at AS full_resolution_at,
  res.calendar_minutes AS full_resolution_time_calendar_minutes,
  res.business_minutes AS full_resolution_time_business_minutes,
  CASE
    WHEN fr.event_at IS NULL
      THEN 'No reply yet'
    ELSE 'Replied'
  END AS reply_status,
  CASE
    WHEN res.event_at IS NULL
      THEN 'Unresolved'
    ELSE 'Resolved'
  END AS resolution_status,
  COALESCE(arc.agent_reply_count, 0) AS agent_reply_count,
  CASE
    WHEN res.event_at IS NOT NULL
      AND COALESCE(arc.agent_reply_count, 0) < 2
      THEN 1
    ELSE 0
  END AS is_one_touch,
  COALESCE(r.reopen_count, 0) AS reopen_count,
  csat.rating_category,
  csat.rating_category IS NOT NULL AS survey_responded,
  -- Excluded from SLA dashboards: Appbot non-English, test groups, and tagged-as-test tickets.
  -- Keep the row but expose a flag so consumers can filter consistently.
  (
    COALESCE(ac.ticket_group, 'All Other Tickets') = 'Appbot - Non-English'
    OR COALESCE(g.name, '') IN ('Sumo Test', 'VPN QA')
    OR tt.ticket_id IS NOT NULL
  ) AS is_excluded_from_sla,
  CURRENT_TIMESTAMP() AS etl_timestamp
FROM
  tickets_in_window AS tw
LEFT JOIN
  business_event_times AS fr
  ON tw.ticket_id = fr.ticket_id
  AND fr.event_type = 'first_reply'
LEFT JOIN
  business_event_times AS res
  ON tw.ticket_id = res.ticket_id
  AND res.event_type = 'full_resolution'
LEFT JOIN
  agent_reply_counts AS arc
  ON tw.ticket_id = arc.ticket_id
LEFT JOIN
  appbot_class AS ac
  ON tw.ticket_id = ac.ticket_id
LEFT JOIN
  automation_tags AS aut
  ON tw.ticket_id = aut.ticket_id
LEFT JOIN
  reopens AS r
  ON tw.ticket_id = r.ticket_id
LEFT JOIN
  csat
  ON tw.ticket_id = csat.ticket_id
LEFT JOIN
  test_tickets AS tt
  ON tw.ticket_id = tt.ticket_id
LEFT JOIN
  `moz-fx-sumo-prod.zendesk.group` AS g
  ON tw.group_id = g.id
