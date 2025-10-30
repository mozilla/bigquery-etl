WITH daily_users AS (
  SELECT
    submission_date,
    client_id,
    sample_id,
    normalized_channel,
    app_version,
    is_desktop,
    is_dau,
  FROM
    `moz-fx-data-shared-prod.telemetry.desktop_active_users`
  WHERE
    submission_date = @submission_date
      -- enterprise is always only "esr" or "release" channels
    AND normalized_channel IN ("release", "esr")
    AND is_daily_user
),
most_recent_client_distribution_and_policy_metrics AS (
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
    ARRAY_AGG(environment.partner.distribution_id IGNORE NULLS ORDER BY submission_timestamp DESC)[
      SAFE_OFFSET(0)
    ] AS distribution_id,
  FROM
    `moz-fx-data-shared-prod.telemetry.main`
  WHERE
    DATE(submission_timestamp)
    BETWEEN DATE_SUB(@submission_date, INTERVAL 27 DAY)
    AND @submission_date
    -- enterprise is always only "esr" or "release" channels
    AND normalized_channel IN ("release", "esr")
  GROUP BY
    ALL
)
SELECT
  @submission_date AS submission_date,
  client_id,
  sample_id,
  normalized_channel,
  app_version,
  distribution_id,
  is_dau,
  is_desktop,
  policies_count,
  policies_is_enterprise,
FROM
  daily_users
INNER JOIN
  most_recent_client_distribution_and_policy_metrics
  USING (client_id, normalized_channel)
