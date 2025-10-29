WITH most_recent_client_policy_metrics AS (
  SELECT
    client_id,
    normalized_channel,
    ARRAY_AGG(
      payload.processes.parent.scalars.policies_count IGNORE NULLS
      ORDER BY
        submission_timestamp DESC
    )[SAFE_OFFSET(0)] AS policies_count,
    ARRAY_AGG(
      payload.processes.parent.scalars.policies_is_enterprise IGNORE NULLS
      ORDER BY
        submission_timestamp DESC
    )[SAFE_OFFSET(0)] AS policies_is_enterprise,
  FROM
    `moz-fx-data-shared-prod.telemetry.main`
  WHERE
    DATE(submission_timestamp)
    BETWEEN DATE_SUB(@submission_date, INTERVAL 27 DAY)
    AND @submission_date
    AND normalized_channel IN ("release", "esr")
  GROUP BY
    ALL
),
baseline_clients AS (
  WITH baseline_active_users AS (
    SELECT
      submission_date,
      client_id,
      sample_id,
      normalized_channel,
      distribution_id,
      is_dau,
      is_daily_user,
    FROM
      `moz-fx-data-shared-prod.telemetry.desktop_active_users`
    WHERE
      submission_date
      BETWEEN DATE_SUB(@submission_date, INTERVAL 27 DAY)
      AND @submission_date
      AND normalized_channel IN ("release", "esr")
  ),
  last_observed_distribution_id_within_27_days AS (
    SELECT
      client_id,
      normalized_channel,
      ARRAY_AGG(distribution_id IGNORE NULLS ORDER BY submission_date DESC)[
        SAFE_OFFSET(0)
      ] AS distribution_id,
    FROM
      baseline_active_users
    GROUP BY
      ALL
  ),
  daily_users AS (
    SELECT
      client_id,
      sample_id,
      normalized_channel,
      is_dau,
    FROM
      baseline_active_users
    WHERE
      submission_date = @submission_date
      AND is_daily_user
  )
  SELECT
    client_id,
    sample_id,
    normalized_channel,
    is_dau,
    distribution_id,
  FROM
    daily_users
  INNER JOIN
    last_observed_distribution_id_within_27_days
    USING (client_id, normalized_channel)
)
SELECT
  @submission_date AS submission_date,
  client_id,
  sample_id,
  normalized_channel,
  is_dau,
  distribution_id,
  policies_count,
  policies_is_enterprise,
FROM
  baseline_clients
LEFT JOIN
  most_recent_client_policy_metrics
  USING (client_id, normalized_channel)
WHERE
  policies_count IS NOT NULL
  AND policies_is_enterprise IS NOT NULL
