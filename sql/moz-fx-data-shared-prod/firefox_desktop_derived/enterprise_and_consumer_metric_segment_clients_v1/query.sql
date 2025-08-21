WITH last_known_client_distribution_within_28_days AS (
  SELECT
    client_id,
    normalized_channel,
    ARRAY_AGG(distribution_id IGNORE NULLS ORDER BY submission_date DESC)[
      SAFE_OFFSET(0)
    ] AS distribution_id,
  FROM
    `moz-fx-data-shared-prod.firefox_desktop.baseline_clients_daily`
  WHERE
    submission_date
    BETWEEN DATE_SUB(@submission_date, INTERVAL 27 DAY)
    AND @submission_date
  GROUP BY
    ALL
),
client_baseline AS (
  SELECT
    client_id,
    sample_id,
    normalized_channel,
    distribution_id,
    is_dau,
  FROM
    `moz-fx-data-shared-prod.firefox_desktop.baseline_active_users`
  LEFT JOIN
    last_known_client_distribution_within_28_days
    USING (client_id, normalized_channel)
  WHERE
    submission_date = @submission_date
    AND is_daily_user
),
client_metrics AS (
  SELECT
    client_info.client_id,
    normalized_channel,
    MAX(metrics.quantity.policies_count) AS policies_count,
    MAX(metrics.boolean.policies_is_enterprise) AS policies_is_enterprise,
  FROM
    `moz-fx-data-shared-prod.firefox_desktop.metrics`
  WHERE
    DATE(submission_timestamp) = @submission_date
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
  -- TODO: it appears sometimes those two values can be null in the result,
  --       should we COALESCE them to 0 and FALSE respectively or should we leave them as null?
  policies_count,
  policies_is_enterprise,
FROM
  client_baseline
LEFT JOIN
  client_metrics
  USING (client_id, normalized_channel)
