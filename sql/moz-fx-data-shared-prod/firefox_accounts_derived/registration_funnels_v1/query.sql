-- extract the relevant fields for each funnel step and segment if necessary
WITH registration_overall_success_reg_view AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id AS client_id,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_frontend.accounts_events
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND metrics.string.event_name = 'reg_view'
),
registration_overall_success_reg_email_code_view AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id AS client_id,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_frontend.accounts_events
  INNER JOIN
    registration_overall_success_reg_view AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = metrics.string.session_flow_id
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND metrics.string.event_name = 'reg_signup_code_view'
),
registration_overall_success_reg_complete AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id AS client_id,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_backend.accounts_events
  INNER JOIN
    registration_overall_success_reg_email_code_view AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = metrics.string.session_flow_id
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND metrics.string.event_name = 'reg_complete'
),
-- aggregate each funnel step value
registration_overall_success_reg_view_aggregated AS (
  SELECT
    submission_date,
    "registration_overall_success" AS funnel,
    COUNT(DISTINCT column) AS aggregated
  FROM
    registration_overall_success_reg_view
  GROUP BY
    submission_date,
    funnel
),
registration_overall_success_reg_email_code_view_aggregated AS (
  SELECT
    submission_date,
    "registration_overall_success" AS funnel,
    COUNT(DISTINCT column) AS aggregated
  FROM
    registration_overall_success_reg_email_code_view
  GROUP BY
    submission_date,
    funnel
),
registration_overall_success_reg_complete_aggregated AS (
  SELECT
    submission_date,
    "registration_overall_success" AS funnel,
    COUNT(DISTINCT column) AS aggregated
  FROM
    registration_overall_success_reg_complete
  GROUP BY
    submission_date,
    funnel
),
-- merge all funnels so results can be written into one table
merged_funnels AS (
  SELECT
    submission_date,
    funnel,
    COALESCE(registration_overall_success_reg_view_aggregated.aggregated) AS reg_view,
    COALESCE(
      registration_overall_success_reg_email_code_view_aggregated.aggregated
    ) AS reg_email_code_view,
    COALESCE(registration_overall_success_reg_complete_aggregated.aggregated) AS reg_complete,
  FROM
    registration_overall_success_reg_view_aggregated
  FULL OUTER JOIN
    registration_overall_success_reg_email_code_view_aggregated
    USING (submission_date, funnel)
  FULL OUTER JOIN
    registration_overall_success_reg_complete_aggregated
    USING (submission_date, funnel)
)
SELECT
  *
FROM
  merged_funnels
