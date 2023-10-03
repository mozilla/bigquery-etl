WITH registration_funnel_engage AS (
  SELECT
    DATE(timestamp) AS submission_date,
    fxa_content_auth_oauth_events AS column
  FROM
    mozdata.firefox_accounts.fxa_content_auth_oauth_events
  WHERE
    DATE(timestamp) = @submission_date
    AND event_type = 'fxa_email_first - engage'
),
registration_funnel_submit AS (
  SELECT
    DATE(timestamp) AS submission_date,
    fxa_content_auth_oauth_events AS column
  FROM
    mozdata.firefox_accounts.fxa_content_auth_oauth_events
  WHERE
    DATE(timestamp) = @submission_date
    AND event_type = 'fxa_email_first - submit'
),
registration_funnel_view AS (
  SELECT
    DATE(timestamp) AS submission_date,
    fxa_content_auth_oauth_events AS column
  FROM
    mozdata.firefox_accounts.fxa_content_auth_oauth_events
  WHERE
    DATE(timestamp) = @submission_date
    AND event_type = 'fxa_email_first - view'
),
registration_funnel_engage_aggregated AS (
  SELECT
    submission_date,
    "registration_funnel" AS funnel,
    COUNT(column) AS aggregated
  FROM
    registration_funnel_engage
  GROUP BY
    submission_date,
    funnel
),
registration_funnel_submit_aggregated AS (
  SELECT
    submission_date,
    "registration_funnel" AS funnel,
    COUNT(column) AS aggregated
  FROM
    registration_funnel_submit
  GROUP BY
    submission_date,
    funnel
),
registration_funnel_view_aggregated AS (
  SELECT
    submission_date,
    "registration_funnel" AS funnel,
    COUNT(column) AS aggregated
  FROM
    registration_funnel_view
  GROUP BY
    submission_date,
    funnel
),
merged_funnels AS (
  SELECT
    submission_date,
    funnel,
    COALESCE(registration_funnel_view_aggregated.aggregated) AS view,
    COALESCE(registration_funnel_submit_aggregated.aggregated) AS submit,
    COALESCE(registration_funnel_engage_aggregated.aggregated) AS engage,
  FROM
    registration_funnel_engage_aggregated
  FULL OUTER JOIN
    registration_funnel_submit_aggregated
  USING
    (submission_date, funnel)
  FULL OUTER JOIN
    registration_funnel_view_aggregated
  USING
    (submission_date, funnel)
)
SELECT
  *
FROM
  merged_funnels
