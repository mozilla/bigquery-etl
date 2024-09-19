-- extract the relevant fields for each funnel step and segment if necessary
WITH login_complete_by_service_login_view AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    metrics.string.session_entrypoint AS entrypoint,
    DATE(submission_timestamp) AS submission_date,
    metrics.string.account_user_id_sha256 AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_frontend.events_stream
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND event = 'login.view'
    AND metrics.string.session_flow_id != ''
),
login_complete_by_service_login_complete AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    prev.entrypoint AS entrypoint,
    DATE(submission_timestamp) AS submission_date,
    metrics.string.account_user_id_sha256 AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_backend.events_stream
  INNER JOIN
    login_complete_by_service_login_view AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = metrics.string.session_flow_id
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND event = 'login.complete'
    AND metrics.string.session_flow_id != ''
),
-- aggregate each funnel step value
login_complete_by_service_login_view_aggregated AS (
  SELECT
    submission_date,
    "login_complete_by_service" AS funnel,
    entrypoint,
    COUNT(DISTINCT column) AS aggregated
  FROM
    login_complete_by_service_login_view
  GROUP BY
    entrypoint,
    submission_date,
    funnel
),
login_complete_by_service_login_complete_aggregated AS (
  SELECT
    submission_date,
    "login_complete_by_service" AS funnel,
    entrypoint,
    COUNT(DISTINCT column) AS aggregated
  FROM
    login_complete_by_service_login_complete
  GROUP BY
    entrypoint,
    submission_date,
    funnel
),
-- merge all funnels so results can be written into one table
merged_funnels AS (
  SELECT
    COALESCE(login_complete_by_service_login_view_aggregated.entrypoint) AS entrypoint,
    submission_date,
    funnel,
    COALESCE(login_complete_by_service_login_view_aggregated.aggregated) AS login_view,
    COALESCE(login_complete_by_service_login_complete_aggregated.aggregated) AS login_complete,
  FROM
    login_complete_by_service_login_view_aggregated
  FULL OUTER JOIN
    login_complete_by_service_login_complete_aggregated
    USING (submission_date, entrypoint, funnel)
)
SELECT
  *
FROM
  merged_funnels
