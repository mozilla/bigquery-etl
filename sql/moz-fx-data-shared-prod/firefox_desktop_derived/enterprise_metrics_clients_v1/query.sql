WITH client_baseline AS (
  WITH active_users_base AS (
    SELECT
      submission_date,
      client_id,
      sample_id,
      legacy_telemetry_client_id,
      channel,
      distribution_id,
      is_daily_user,
      is_dau,
    FROM
      `moz-fx-data-shared-prod.firefox_desktop.baseline_active_users`
    WHERE
      submission_date
      BETWEEN DATE_SUB(@submission_date, INTERVAL 27 DAY)
      AND @submission_date
  ),
  last_observed_distribution_id_within_27_days AS (
    SELECT
      client_id,
      sample_id,
      channel,
      ARRAY_AGG(distribution_id IGNORE NULLS ORDER BY submission_date DESC)[
        SAFE_OFFSET(0)
      ] AS distribution_id,
    FROM
      active_users_base
    GROUP BY
      ALL
  ),
  daily_users AS (
    SELECT
      client_id,
      sample_id,
      legacy_telemetry_client_id,
      channel,
      is_dau,
    FROM
      active_users_base
    WHERE
      submission_date = @submission_date
      AND is_daily_user
  )
  SELECT
    client_id,
    sample_id,
    legacy_telemetry_client_id,
    channel AS normalized_channel,
    distribution_id,
    is_dau,
  FROM
    daily_users
  LEFT JOIN
    last_observed_distribution_id_within_27_days
    USING (client_id, sample_id, channel)
),
most_recent_client_policy_metrics AS (
  SELECT
    client_info.client_id,
    normalized_channel,
    ARRAY_AGG(metrics.quantity.policies_count IGNORE NULLS ORDER BY submission_timestamp DESC)[
      SAFE_OFFSET(0)
    ] AS policies_count,
    ARRAY_AGG(
      metrics.boolean.policies_is_enterprise IGNORE NULLS
      ORDER BY
        submission_timestamp DESC
    )[SAFE_OFFSET(0)] AS policies_is_enterprise,
  FROM
    `moz-fx-data-shared-prod.firefox_desktop.metrics`
  WHERE
    DATE(submission_timestamp)
    BETWEEN DATE_SUB(@submission_date, INTERVAL 27 DAY)
    AND @submission_date
  GROUP BY
    ALL
)
SELECT
  @submission_date AS submission_date,
  client_id,
  sample_id,
  normalized_channel,
  distribution_id,
  is_dau,
  policies_count,
  policies_is_enterprise,
  legacy_telemetry_client_id,
FROM
  client_baseline
LEFT JOIN
  most_recent_client_policy_metrics
  USING (client_id, normalized_channel)
WHERE
  -- enterprise is always only "esr" or "release" channels
  normalized_channel IN ("release", "esr")
