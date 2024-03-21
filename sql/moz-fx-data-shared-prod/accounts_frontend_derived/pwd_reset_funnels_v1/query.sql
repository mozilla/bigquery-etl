-- extract the relevant fields for each funnel step and segment if necessary
WITH pwd_reset_without_recovery_key_pwd_reset_view AS (
  SELECT
    client_info.client_id AS join_key,
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id AS client_id,
    client_info.client_id AS column
  FROM
    mozdata.accounts_frontend.accounts_events
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND metrics.string.event_name = 'password_reset_view'
),
pwd_reset_without_recovery_key_create_new_pwd_view_no_rk AS (
  SELECT
    client_info.client_id AS join_key,
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id AS client_id,
    client_info.client_id AS column
  FROM
    mozdata.accounts_frontend.accounts_events
  INNER JOIN
    pwd_reset_without_recovery_key_pwd_reset_view AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = client_info.client_id
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND metrics.string.event_name = 'password_reset_create_new_view'
),
pwd_reset_without_recovery_key_pwd_reset_success_no_rk AS (
  SELECT
    client_info.client_id AS join_key,
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id AS client_id,
    client_info.client_id AS column
  FROM
    mozdata.accounts_frontend.accounts_events
  INNER JOIN
    pwd_reset_without_recovery_key_create_new_pwd_view_no_rk AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = client_info.client_id
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND metrics.string.event_name = 'password_reset_create_new_success_view'
),
pwd_reset_with_recovery_key_pwd_reset_view AS (
  SELECT
    client_info.client_id AS join_key,
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id AS client_id,
    client_info.client_id AS column
  FROM
    mozdata.accounts_frontend.accounts_events
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND metrics.string.event_name = 'password_reset_view'
),
pwd_reset_with_recovery_key_create_new_pwd_view_with_rk AS (
  SELECT
    client_info.client_id AS join_key,
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id AS client_id,
    client_info.client_id AS column
  FROM
    mozdata.accounts_frontend.accounts_events
  INNER JOIN
    pwd_reset_with_recovery_key_pwd_reset_view AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = client_info.client_id
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND metrics.string.event_name = 'password_reset_recovery_key_create_new_view'
),
pwd_reset_with_recovery_key_pwd_reset_success_with_rk AS (
  SELECT
    client_info.client_id AS join_key,
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id AS client_id,
    client_info.client_id AS column
  FROM
    mozdata.accounts_frontend.accounts_events
  INNER JOIN
    pwd_reset_with_recovery_key_create_new_pwd_view_with_rk AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = client_info.client_id
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND metrics.string.event_name = 'password_reset_recovery_key_create_success_view'
),
-- aggregate each funnel step value
pwd_reset_without_recovery_key_pwd_reset_view_aggregated AS (
  SELECT
    submission_date,
    "pwd_reset_without_recovery_key" AS funnel,
    COUNT(DISTINCT column) AS aggregated
  FROM
    pwd_reset_without_recovery_key_pwd_reset_view
  GROUP BY
    submission_date,
    funnel
),
pwd_reset_without_recovery_key_create_new_pwd_view_no_rk_aggregated AS (
  SELECT
    submission_date,
    "pwd_reset_without_recovery_key" AS funnel,
    COUNT(DISTINCT column) AS aggregated
  FROM
    pwd_reset_without_recovery_key_create_new_pwd_view_no_rk
  GROUP BY
    submission_date,
    funnel
),
pwd_reset_without_recovery_key_pwd_reset_success_no_rk_aggregated AS (
  SELECT
    submission_date,
    "pwd_reset_without_recovery_key" AS funnel,
    COUNT(DISTINCT column) AS aggregated
  FROM
    pwd_reset_without_recovery_key_pwd_reset_success_no_rk
  GROUP BY
    submission_date,
    funnel
),
pwd_reset_with_recovery_key_pwd_reset_view_aggregated AS (
  SELECT
    submission_date,
    "pwd_reset_with_recovery_key" AS funnel,
    COUNT(DISTINCT column) AS aggregated
  FROM
    pwd_reset_with_recovery_key_pwd_reset_view
  GROUP BY
    submission_date,
    funnel
),
pwd_reset_with_recovery_key_create_new_pwd_view_with_rk_aggregated AS (
  SELECT
    submission_date,
    "pwd_reset_with_recovery_key" AS funnel,
    COUNT(DISTINCT column) AS aggregated
  FROM
    pwd_reset_with_recovery_key_create_new_pwd_view_with_rk
  GROUP BY
    submission_date,
    funnel
),
pwd_reset_with_recovery_key_pwd_reset_success_with_rk_aggregated AS (
  SELECT
    submission_date,
    "pwd_reset_with_recovery_key" AS funnel,
    COUNT(DISTINCT column) AS aggregated
  FROM
    pwd_reset_with_recovery_key_pwd_reset_success_with_rk
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
      pwd_reset_without_recovery_key_pwd_reset_view_aggregated.aggregated,
      pwd_reset_with_recovery_key_pwd_reset_view_aggregated.aggregated
    ) AS pwd_reset_view,
    COALESCE(
      pwd_reset_without_recovery_key_create_new_pwd_view_no_rk_aggregated.aggregated,
      NULL
    ) AS create_new_pwd_view_no_rk,
    COALESCE(
      pwd_reset_without_recovery_key_pwd_reset_success_no_rk_aggregated.aggregated,
      NULL
    ) AS pwd_reset_success_no_rk,
    COALESCE(
      NULL,
      pwd_reset_with_recovery_key_create_new_pwd_view_with_rk_aggregated.aggregated
    ) AS create_new_pwd_view_with_rk,
    COALESCE(
      NULL,
      pwd_reset_with_recovery_key_pwd_reset_success_with_rk_aggregated.aggregated
    ) AS pwd_reset_success_with_rk,
  FROM
    pwd_reset_without_recovery_key_pwd_reset_view_aggregated
  FULL OUTER JOIN
    pwd_reset_without_recovery_key_create_new_pwd_view_no_rk_aggregated
    USING (submission_date, funnel)
  FULL OUTER JOIN
    pwd_reset_without_recovery_key_pwd_reset_success_no_rk_aggregated
    USING (submission_date, funnel)
  FULL OUTER JOIN
    pwd_reset_with_recovery_key_pwd_reset_view_aggregated
    USING (submission_date, funnel)
  FULL OUTER JOIN
    pwd_reset_with_recovery_key_create_new_pwd_view_with_rk_aggregated
    USING (submission_date, funnel)
  FULL OUTER JOIN
    pwd_reset_with_recovery_key_pwd_reset_success_with_rk_aggregated
    USING (submission_date, funnel)
)
SELECT
  *
FROM
  merged_funnels
