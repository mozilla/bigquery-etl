-- extract the relevant fields for each funnel step and segment if necessary
WITH email_first_login_success_email_first_view AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id AS client_id,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_frontend.accounts_events
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND metrics.string.event_name = 'email_first_view'
),
email_first_login_success_login_view AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id AS client_id,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_frontend.accounts_events
  INNER JOIN
    email_first_login_success_email_first_view AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = metrics.string.session_flow_id
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND metrics.string.event_name = 'login_view'
),
email_first_login_success_login_success AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id AS client_id,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_frontend.accounts_events
  INNER JOIN
    email_first_login_success_login_view AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = metrics.string.session_flow_id
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND metrics.string.event_name = 'login_submit_success'
),
email_first_registration_success_email_first_view AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id AS client_id,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_frontend.accounts_events
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND metrics.string.event_name = 'email_first_view'
),
email_first_registration_success_reg_view AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id AS client_id,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_frontend.accounts_events
  INNER JOIN
    email_first_registration_success_email_first_view AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = metrics.string.session_flow_id
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND metrics.string.event_name = 'reg_view'
),
email_first_registration_success_reg_success AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id AS client_id,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_backend.accounts_events
  INNER JOIN
    email_first_registration_success_reg_view AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = metrics.string.session_flow_id
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND metrics.string.event_name = 'reg_complete'
),
-- aggregate each funnel step value
email_first_login_success_email_first_view_aggregated AS (
  SELECT
    submission_date,
    "email_first_login_success" AS funnel,
    COUNT(DISTINCT column) AS aggregated
  FROM
    email_first_login_success_email_first_view
  GROUP BY
    submission_date,
    funnel
),
email_first_login_success_login_view_aggregated AS (
  SELECT
    submission_date,
    "email_first_login_success" AS funnel,
    COUNT(DISTINCT column) AS aggregated
  FROM
    email_first_login_success_login_view
  GROUP BY
    submission_date,
    funnel
),
email_first_login_success_login_success_aggregated AS (
  SELECT
    submission_date,
    "email_first_login_success" AS funnel,
    COUNT(DISTINCT column) AS aggregated
  FROM
    email_first_login_success_login_success
  GROUP BY
    submission_date,
    funnel
),
email_first_registration_success_email_first_view_aggregated AS (
  SELECT
    submission_date,
    "email_first_registration_success" AS funnel,
    COUNT(DISTINCT column) AS aggregated
  FROM
    email_first_registration_success_email_first_view
  GROUP BY
    submission_date,
    funnel
),
email_first_registration_success_reg_view_aggregated AS (
  SELECT
    submission_date,
    "email_first_registration_success" AS funnel,
    COUNT(DISTINCT column) AS aggregated
  FROM
    email_first_registration_success_reg_view
  GROUP BY
    submission_date,
    funnel
),
email_first_registration_success_reg_success_aggregated AS (
  SELECT
    submission_date,
    "email_first_registration_success" AS funnel,
    COUNT(DISTINCT column) AS aggregated
  FROM
    email_first_registration_success_reg_success
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
      email_first_login_success_email_first_view_aggregated.aggregated,
      email_first_registration_success_email_first_view_aggregated.aggregated
    ) AS email_first_view,
    COALESCE(email_first_login_success_login_view_aggregated.aggregated, NULL) AS login_view,
    COALESCE(email_first_login_success_login_success_aggregated.aggregated, NULL) AS login_success,
    COALESCE(NULL, email_first_registration_success_reg_view_aggregated.aggregated) AS reg_view,
    COALESCE(
      NULL,
      email_first_registration_success_reg_success_aggregated.aggregated
    ) AS reg_success,
  FROM
    email_first_login_success_email_first_view_aggregated
  FULL OUTER JOIN
    email_first_login_success_login_view_aggregated
    USING (submission_date, funnel)
  FULL OUTER JOIN
    email_first_login_success_login_success_aggregated
    USING (submission_date, funnel)
  FULL OUTER JOIN
    email_first_registration_success_email_first_view_aggregated
    USING (submission_date, funnel)
  FULL OUTER JOIN
    email_first_registration_success_reg_view_aggregated
    USING (submission_date, funnel)
  FULL OUTER JOIN
    email_first_registration_success_reg_success_aggregated
    USING (submission_date, funnel)
)
SELECT
  *
FROM
  merged_funnels
