-- Daily, weekly and monthly active users of the Deepnote workspaces, per workspace and
-- across all workspaces (workspace_id = 'all'; distinct users can't be summed across
-- workspaces). Ephemeral API key churn (created by MCP/API clients) does not count as
-- activity.
WITH events AS (
  -- every event counts towards its own workspace and towards 'all'
  SELECT
    audit_log.* EXCEPT (workspace_id),
    rollup_workspace_id AS workspace_id,
  FROM
    `moz-fx-data-shared-prod.deepnote_derived.audit_log_v1` AS audit_log
  CROSS JOIN
    UNNEST([audit_log.workspace_id, 'all']) AS rollup_workspace_id
),
activity AS (
  SELECT DISTINCT
    workspace_id,
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
    events
  WHERE
    user_id IS NOT NULL
    AND event_type NOT IN ('api_key_created', 'api_key_deleted', 'api_key_viewed')
),
-- every day from a workspace's first audit log event on; days without events are quiet
dates AS (
  SELECT
    workspace_id,
    submission_date,
  FROM
    (
      SELECT
        workspace_id,
        MIN(DATE(created_at)) AS first_date,
        MAX(MAX(DATE(created_at))) OVER () AS last_date,
      FROM
        events
      GROUP BY
        workspace_id
    )
  CROSS JOIN
    UNNEST(GENERATE_DATE_ARRAY(first_date, last_date)) AS submission_date
),
member_events AS (
  SELECT
    workspace_id,
    DATE(created_at) AS submission_date,
    COUNTIF(event_type = 'workspace_member_added') AS members_added,
    COUNTIF(event_type = 'workspace_member_deleted') AS members_removed,
  FROM
    events
  GROUP BY
    workspace_id,
    submission_date
),
member_counts AS (
  SELECT
    rollup_workspace_id AS workspace_id,
    snapshot_date AS submission_date,
    COUNT(DISTINCT IF(access_level != 'guest', user_id, NULL)) AS members,
    COUNT(DISTINCT IF(access_level = 'admin', user_id, NULL)) AS admins,
    COUNT(DISTINCT IF(access_level = 'editor', user_id, NULL)) AS editors,
    COUNT(DISTINCT IF(access_level IN ('viewer', 'restricted_user'), user_id, NULL)) AS viewers,
    COUNT(DISTINCT IF(access_level = 'guest', user_id, NULL)) AS guests,
  FROM
    `moz-fx-data-shared-prod.deepnote_derived.members_v1` AS members
  CROSS JOIN
    UNNEST([members.workspace_id, 'all']) AS rollup_workspace_id
  GROUP BY
    workspace_id,
    submission_date
),
first_seen AS (
  SELECT
    workspace_id,
    first_seen_date AS submission_date,
    COUNT(*) AS first_seen_users,
  FROM
    (
      SELECT
        workspace_id,
        user_id,
        MIN(activity_date) AS first_seen_date,
      FROM
        activity
      GROUP BY
        workspace_id,
        user_id
    )
  GROUP BY
    workspace_id,
    submission_date
),
daily AS (
  SELECT
    dates.submission_date,
    dates.workspace_id,
    COUNT(DISTINCT IF(activity_date = dates.submission_date, user_id, NULL)) AS dau,
    COUNT(
      DISTINCT IF(activity_date = dates.submission_date AND channel = 'ui', user_id, NULL)
    ) AS dau_ui,
    COUNT(
      DISTINCT IF(activity_date = dates.submission_date AND channel = 'api', user_id, NULL)
    ) AS dau_api,
    COUNT(
      DISTINCT IF(activity_date = dates.submission_date AND is_creating, user_id, NULL)
    ) AS dau_creators,
    COUNT(
      DISTINCT IF(activity_date > DATE_SUB(dates.submission_date, INTERVAL 7 DAY), user_id, NULL)
    ) AS wau,
    COUNT(DISTINCT user_id) AS mau,
    IFNULL(ANY_VALUE(first_seen.first_seen_users), 0) AS first_seen_users,
    IFNULL(ANY_VALUE(member_events.members_added), 0) AS members_added,
    IFNULL(ANY_VALUE(member_events.members_removed), 0) AS members_removed,
    ANY_VALUE(member_counts.members) AS members,
    ANY_VALUE(member_counts.admins) AS admins,
    ANY_VALUE(member_counts.editors) AS editors,
    ANY_VALUE(member_counts.viewers) AS viewers,
    ANY_VALUE(member_counts.guests) AS guests,
  FROM
    dates
  LEFT JOIN
    activity
    ON activity.workspace_id = dates.workspace_id
    AND activity.activity_date
    BETWEEN DATE_SUB(dates.submission_date, INTERVAL 27 DAY)
    AND dates.submission_date
  LEFT JOIN
    member_events
    ON member_events.workspace_id = dates.workspace_id
    AND member_events.submission_date = dates.submission_date
  LEFT JOIN
    member_counts
    ON member_counts.workspace_id = dates.workspace_id
    AND member_counts.submission_date = dates.submission_date
  LEFT JOIN
    first_seen
    ON first_seen.workspace_id = dates.workspace_id
    AND first_seen.submission_date = dates.submission_date
  GROUP BY
    dates.submission_date,
    dates.workspace_id
)
SELECT
  *,
  -- each user is counted once, on the date of their first audit log event
  SUM(first_seen_users) OVER (PARTITION BY workspace_id ORDER BY submission_date) AS total_users,
FROM
  daily
