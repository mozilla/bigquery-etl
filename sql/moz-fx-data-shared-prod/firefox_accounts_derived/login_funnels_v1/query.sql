-- extract the relevant fields for each funnel step and segment if necessary
WITH login_overall_success_login_view AS (
  SELECT
    flow_id AS join_key,
    DATE(timestamp) AS submission_date,
    user_id AS client_id,
    flow_id AS column
  FROM
    mozdata.firefox_accounts.fxa_all_events
  WHERE
    DATE(timestamp) = @submission_date
    AND event_type = 'fxa_login - view'
),
login_overall_success_login_success AS (
  SELECT
    flow_id AS join_key,
    DATE(timestamp) AS submission_date,
    user_id AS client_id,
    flow_id AS column
  FROM
    mozdata.firefox_accounts.fxa_all_events
  INNER JOIN
    login_overall_success_login_view AS prev
  ON
    prev.submission_date = DATE(timestamp)
    AND prev.join_key = flow_id
  WHERE
    DATE(timestamp) = @submission_date
    AND event_type = 'fxa_login - complete'
),
login_email_confirmation_login_view AS (
  SELECT
    flow_id AS join_key,
    DATE(timestamp) AS submission_date,
    user_id AS client_id,
    flow_id AS column
  FROM
    mozdata.firefox_accounts.fxa_all_events
  WHERE
    DATE(timestamp) = @submission_date
    AND event_type = 'fxa_login - view'
),
login_email_confirmation_confirm_email AS (
  SELECT
    flow_id AS join_key,
    DATE(timestamp) AS submission_date,
    user_id AS client_id,
    flow_id AS column
  FROM
    mozdata.firefox_accounts.fxa_all_events
  INNER JOIN
    login_email_confirmation_login_view AS prev
  ON
    prev.submission_date = DATE(timestamp)
    AND prev.join_key = flow_id
  WHERE
    DATE(timestamp) = @submission_date
    AND event_type = 'fxa_email - sent'
    AND JSON_VALUE(event_properties, '$.email_type') = 'login'
),
login_email_confirmation_login_success AS (
  SELECT
    flow_id AS join_key,
    DATE(timestamp) AS submission_date,
    user_id AS client_id,
    flow_id AS column
  FROM
    mozdata.firefox_accounts.fxa_all_events
  INNER JOIN
    login_email_confirmation_confirm_email AS prev
  ON
    prev.submission_date = DATE(timestamp)
    AND prev.join_key = flow_id
  WHERE
    DATE(timestamp) = @submission_date
    AND event_type = 'fxa_login - complete'
),
login_2fa_login_view AS (
  SELECT
    flow_id AS join_key,
    DATE(timestamp) AS submission_date,
    user_id AS client_id,
    flow_id AS column
  FROM
    mozdata.firefox_accounts.fxa_all_events
  WHERE
    DATE(timestamp) = @submission_date
    AND event_type = 'fxa_login - view'
),
login_2fa_login_confirm_2fa AS (
  SELECT
    flow_id AS join_key,
    DATE(timestamp) AS submission_date,
    user_id AS client_id,
    flow_id AS column
  FROM
    mozdata.firefox_accounts.fxa_all_events
  INNER JOIN
    login_2fa_login_view AS prev
  ON
    prev.submission_date = DATE(timestamp)
    AND prev.join_key = flow_id
  WHERE
    DATE(timestamp) = @submission_date
    AND event_type = 'fxa_login - totp_code_view'
),
login_2fa_login_success AS (
  SELECT
    flow_id AS join_key,
    DATE(timestamp) AS submission_date,
    user_id AS client_id,
    flow_id AS column
  FROM
    mozdata.firefox_accounts.fxa_all_events
  INNER JOIN
    login_2fa_login_confirm_2fa AS prev
  ON
    prev.submission_date = DATE(timestamp)
    AND prev.join_key = flow_id
  WHERE
    DATE(timestamp) = @submission_date
    AND event_type = 'fxa_login - complete'
),
-- aggregate each funnel step value
login_overall_success_login_view_aggregated AS (
  SELECT
    submission_date,
    "login_overall_success" AS funnel,
    COUNT(DISTINCT column) AS aggregated
  FROM
    login_overall_success_login_view
  GROUP BY
    submission_date,
    funnel
),
login_overall_success_login_success_aggregated AS (
  SELECT
    submission_date,
    "login_overall_success" AS funnel,
    COUNT(DISTINCT column) AS aggregated
  FROM
    login_overall_success_login_success
  GROUP BY
    submission_date,
    funnel
),
login_email_confirmation_login_view_aggregated AS (
  SELECT
    submission_date,
    "login_email_confirmation" AS funnel,
    COUNT(DISTINCT column) AS aggregated
  FROM
    login_email_confirmation_login_view
  GROUP BY
    submission_date,
    funnel
),
login_email_confirmation_confirm_email_aggregated AS (
  SELECT
    submission_date,
    "login_email_confirmation" AS funnel,
    COUNT(DISTINCT column) AS aggregated
  FROM
    login_email_confirmation_confirm_email
  GROUP BY
    submission_date,
    funnel
),
login_email_confirmation_login_success_aggregated AS (
  SELECT
    submission_date,
    "login_email_confirmation" AS funnel,
    COUNT(DISTINCT column) AS aggregated
  FROM
    login_email_confirmation_login_success
  GROUP BY
    submission_date,
    funnel
),
login_2fa_login_view_aggregated AS (
  SELECT
    submission_date,
    "login_2fa" AS funnel,
    COUNT(DISTINCT column) AS aggregated
  FROM
    login_2fa_login_view
  GROUP BY
    submission_date,
    funnel
),
login_2fa_login_confirm_2fa_aggregated AS (
  SELECT
    submission_date,
    "login_2fa" AS funnel,
    COUNT(DISTINCT column) AS aggregated
  FROM
    login_2fa_login_confirm_2fa
  GROUP BY
    submission_date,
    funnel
),
login_2fa_login_success_aggregated AS (
  SELECT
    submission_date,
    "login_2fa" AS funnel,
    COUNT(DISTINCT column) AS aggregated
  FROM
    login_2fa_login_success
  GROUP BY
    submission_date,
    funnel
),
-- merge all funnels so results can be written into one table
merged_funnels AS (
  SELECT
    submission_date,
    funnel,
    COALESCE(
      login_overall_success_login_view_aggregated.aggregated,
      login_email_confirmation_login_view_aggregated.aggregated,
      login_2fa_login_view_aggregated.aggregated
    ) AS login_view,
    COALESCE(
      NULL,
      login_email_confirmation_confirm_email_aggregated.aggregated,
      NULL
    ) AS confirm_email,
    COALESCE(NULL, NULL, login_2fa_login_confirm_2fa_aggregated.aggregated) AS login_confirm_2fa,
    COALESCE(
      login_overall_success_login_success_aggregated.aggregated,
      login_email_confirmation_login_success_aggregated.aggregated,
      login_2fa_login_success_aggregated.aggregated
    ) AS login_success,
  FROM
    login_overall_success_login_view_aggregated
  FULL OUTER JOIN
    login_overall_success_login_success_aggregated
  USING
    (submission_date, funnel)
  FULL OUTER JOIN
    login_email_confirmation_login_view_aggregated
  USING
    (submission_date, funnel)
  FULL OUTER JOIN
    login_email_confirmation_confirm_email_aggregated
  USING
    (submission_date, funnel)
  FULL OUTER JOIN
    login_email_confirmation_login_success_aggregated
  USING
    (submission_date, funnel)
  FULL OUTER JOIN
    login_2fa_login_view_aggregated
  USING
    (submission_date, funnel)
  FULL OUTER JOIN
    login_2fa_login_confirm_2fa_aggregated
  USING
    (submission_date, funnel)
  FULL OUTER JOIN
    login_2fa_login_success_aggregated
  USING
    (submission_date, funnel)
)
SELECT
  *
FROM
  merged_funnels
