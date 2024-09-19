-- extract the relevant fields for each funnel step and segment if necessary
WITH pwd_reset_without_recovery_key_pwd_reset_view AS (
  SELECT
    client_id AS join_key,
    IF(
      COALESCE(
        NULLIF(metrics.string.relying_party_oauth_client_id, ''),
        NULLIF(metrics.string.relying_party_service, '')
      ) IN ('sync', '5882386c6d801776'),
      'Sync',
      'Non-Sync'
    ) AS service,
    DATE(submission_timestamp) AS submission_date,
    client_id AS client_id_column,
    client_id AS column
  FROM
    mozdata.accounts_frontend.events_stream
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND event = 'password_reset.view'
),
pwd_reset_without_recovery_key_create_new_pwd_view_no_rk AS (
  SELECT
    client_id AS join_key,
    prev.service AS service,
    DATE(submission_timestamp) AS submission_date,
    client_id AS client_id_column,
    client_id AS column
  FROM
    mozdata.accounts_frontend.events_stream
  INNER JOIN
    pwd_reset_without_recovery_key_pwd_reset_view AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = client_id
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND event = 'password_reset.create_new_view'
),
pwd_reset_without_recovery_key_pwd_reset_success_no_rk AS (
  SELECT
    client_id AS join_key,
    prev.service AS service,
    DATE(submission_timestamp) AS submission_date,
    client_id AS client_id_column,
    client_id AS column
  FROM
    mozdata.accounts_frontend.events_stream
  INNER JOIN
    pwd_reset_without_recovery_key_create_new_pwd_view_no_rk AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = client_id
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND event = 'password_reset.create_new_success_view'
),
pwd_reset_with_recovery_key_pwd_reset_view AS (
  SELECT
    client_id AS join_key,
    IF(
      COALESCE(
        NULLIF(metrics.string.relying_party_oauth_client_id, ''),
        NULLIF(metrics.string.relying_party_service, '')
      ) IN ('sync', '5882386c6d801776'),
      'Sync',
      'Non-Sync'
    ) AS service,
    DATE(submission_timestamp) AS submission_date,
    client_id AS client_id_column,
    client_id AS column
  FROM
    mozdata.accounts_frontend.events_stream
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND event = 'password_reset.view'
),
pwd_reset_with_recovery_key_create_new_pwd_view_with_rk AS (
  SELECT
    client_id AS join_key,
    prev.service AS service,
    DATE(submission_timestamp) AS submission_date,
    client_id AS client_id_column,
    client_id AS column
  FROM
    mozdata.accounts_frontend.events_stream
  INNER JOIN
    pwd_reset_with_recovery_key_pwd_reset_view AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = client_id
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND event = 'password_reset.recovery_key_create_new_view'
),
pwd_reset_with_recovery_key_pwd_reset_success_with_rk AS (
  SELECT
    client_id AS join_key,
    prev.service AS service,
    DATE(submission_timestamp) AS submission_date,
    client_id AS client_id_column,
    client_id AS column
  FROM
    mozdata.accounts_frontend.events_stream
  INNER JOIN
    pwd_reset_with_recovery_key_create_new_pwd_view_with_rk AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = client_id
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND event = 'password_reset.recovery_key_create_success_view'
),
-- aggregate each funnel step value
pwd_reset_without_recovery_key_pwd_reset_view_aggregated AS (
  SELECT
    submission_date,
    "pwd_reset_without_recovery_key" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    pwd_reset_without_recovery_key_pwd_reset_view
  GROUP BY
    service,
    submission_date,
    funnel
),
pwd_reset_without_recovery_key_create_new_pwd_view_no_rk_aggregated AS (
  SELECT
    submission_date,
    "pwd_reset_without_recovery_key" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    pwd_reset_without_recovery_key_create_new_pwd_view_no_rk
  GROUP BY
    service,
    submission_date,
    funnel
),
pwd_reset_without_recovery_key_pwd_reset_success_no_rk_aggregated AS (
  SELECT
    submission_date,
    "pwd_reset_without_recovery_key" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    pwd_reset_without_recovery_key_pwd_reset_success_no_rk
  GROUP BY
    service,
    submission_date,
    funnel
),
pwd_reset_with_recovery_key_pwd_reset_view_aggregated AS (
  SELECT
    submission_date,
    "pwd_reset_with_recovery_key" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    pwd_reset_with_recovery_key_pwd_reset_view
  GROUP BY
    service,
    submission_date,
    funnel
),
pwd_reset_with_recovery_key_create_new_pwd_view_with_rk_aggregated AS (
  SELECT
    submission_date,
    "pwd_reset_with_recovery_key" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    pwd_reset_with_recovery_key_create_new_pwd_view_with_rk
  GROUP BY
    service,
    submission_date,
    funnel
),
pwd_reset_with_recovery_key_pwd_reset_success_with_rk_aggregated AS (
  SELECT
    submission_date,
    "pwd_reset_with_recovery_key" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    pwd_reset_with_recovery_key_pwd_reset_success_with_rk
  GROUP BY
    service,
    submission_date,
    funnel
),
-- merge all funnels so results can be written into one table
merged_funnels AS (
  SELECT
    COALESCE(
      pwd_reset_without_recovery_key_pwd_reset_view_aggregated.service,
      pwd_reset_with_recovery_key_pwd_reset_view_aggregated.service
    ) AS service,
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
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    pwd_reset_without_recovery_key_pwd_reset_success_no_rk_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    pwd_reset_with_recovery_key_pwd_reset_view_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    pwd_reset_with_recovery_key_create_new_pwd_view_with_rk_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    pwd_reset_with_recovery_key_pwd_reset_success_with_rk_aggregated
    USING (submission_date, service, funnel)
)
SELECT
  *
FROM
  merged_funnels
