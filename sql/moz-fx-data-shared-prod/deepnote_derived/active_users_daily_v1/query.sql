-- Daily, weekly and monthly active users of the default Deepnote workspace.
-- Ephemeral API key churn (created by MCP/API clients) does not count as activity.
WITH activity AS (
  SELECT DISTINCT
    DATE(created_at) AS activity_date,
    user_id,
    -- browser user agents are the Deepnote UI (actor_type labels API echoes as "user")
    IF(STARTS_WITH(IFNULL(user_agent, ''), 'Mozilla/'), 'ui', 'api') AS channel,
    event_type IN (
      'notebook_edited',
      'block_created',
      'block_deleted',
      'blocks_reordered',
      'file_created',
      'file_edited',
      'file_uploaded',
      'notebook_executed',
      'project_created',
      'streamlit_app_created',
      'static_site_published'
    ) AS is_creating,
  FROM
    `moz-fx-data-shared-prod.deepnote_derived.audit_log_v1`
  WHERE
    user_id IS NOT NULL
    AND event_type NOT IN ('api_key_created', 'api_key_deleted', 'api_key_viewed')
),
-- only dates within the range of an audit log export (exports may leave gaps between
-- them; days without events inside an export are covered, just quiet)
dates AS (
  SELECT DISTINCT
    submission_date
  FROM
    (
      SELECT
        MIN(DATE(created_at)) AS first_date,
        MAX(DATE(created_at)) AS last_date,
      FROM
        `moz-fx-data-shared-prod.deepnote_derived.audit_log_v1`
      GROUP BY
        source_file
    )
  CROSS JOIN
    UNNEST(GENERATE_DATE_ARRAY(first_date, last_date)) AS submission_date
),
coverage AS (
  SELECT
    dates.submission_date,
    COUNT(*) AS covered_days_28d,
  FROM
    dates
  JOIN
    dates AS window_dates
    ON window_dates.submission_date
    BETWEEN DATE_SUB(dates.submission_date, INTERVAL 27 DAY)
    AND dates.submission_date
  GROUP BY
    dates.submission_date
),
members AS (
  SELECT
    DATE(created_at) AS submission_date,
    COUNTIF(event_type = 'workspace_member_added') AS members_added,
    COUNTIF(event_type = 'workspace_member_deleted') AS members_removed,
  FROM
    `moz-fx-data-shared-prod.deepnote_derived.audit_log_v1`
  GROUP BY
    submission_date
),
first_seen AS (
  SELECT
    first_seen_date AS submission_date,
    COUNT(*) AS first_seen_users,
  FROM
    (SELECT user_id, MIN(activity_date) AS first_seen_date FROM activity GROUP BY user_id)
  GROUP BY
    submission_date
),
daily AS (
  SELECT
    submission_date,
    COUNT(DISTINCT IF(activity_date = submission_date, user_id, NULL)) AS dau,
    COUNT(DISTINCT IF(activity_date = submission_date AND channel = 'ui', user_id, NULL)) AS dau_ui,
    COUNT(
      DISTINCT IF(activity_date = submission_date AND channel = 'api', user_id, NULL)
    ) AS dau_api,
    COUNT(
      DISTINCT IF(activity_date = submission_date AND is_creating, user_id, NULL)
    ) AS dau_creators,
    COUNT(
      DISTINCT IF(activity_date > DATE_SUB(submission_date, INTERVAL 7 DAY), user_id, NULL)
    ) AS wau,
    COUNT(DISTINCT user_id) AS mau,
    IFNULL(ANY_VALUE(first_seen.first_seen_users), 0) AS first_seen_users,
    IFNULL(ANY_VALUE(members.members_added), 0) AS members_added,
    IFNULL(ANY_VALUE(members.members_removed), 0) AS members_removed,
      -- WAU/MAU undercount unless the audit log covers the full 28 day window
    ANY_VALUE(coverage.covered_days_28d) AS covered_days_28d,
    ANY_VALUE(coverage.covered_days_28d) = 28 AS is_mau_complete,
  FROM
    dates
  LEFT JOIN
    activity
    ON activity.activity_date
    BETWEEN DATE_SUB(dates.submission_date, INTERVAL 27 DAY)
    AND dates.submission_date
  LEFT JOIN
    members
    USING (submission_date)
  LEFT JOIN
    first_seen
    USING (submission_date)
  JOIN
    coverage
    USING (submission_date)
  GROUP BY
    submission_date
)
SELECT
  *,
  -- each user is counted once, on the date of their first audit log event
  SUM(first_seen_users) OVER (ORDER BY submission_date) AS total_users,
FROM
  daily
