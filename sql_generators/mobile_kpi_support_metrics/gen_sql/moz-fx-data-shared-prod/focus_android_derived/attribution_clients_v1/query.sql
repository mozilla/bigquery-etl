-- Query generated via `mobile_kpi_support_metrics` SQL generator.
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.focus_android.attribution_clients`
AS
WITH new_profiles AS (
  SELECT
    submission_date,
    client_id,
    sample_id,
  FROM
    `moz-fx-data-shared-prod.focus_android.active_users`
  WHERE
    submission_date = @submission_date
    AND is_new_profile
),
metrics_ping_base AS (
  SELECT
    client_info.client_id AS client_id,
    sample_id,
    submission_timestamp,
    NULLIF(metrics.string.browser_install_source, "") AS install_source,
  FROM
    `moz-fx-data-shared-prod.focus_android.metrics` AS fxa_metrics
  WHERE
    DATE(submission_timestamp)
    BETWEEN DATE_SUB(@submission_date, INTERVAL 1 DAY)
    AND DATE_ADD(@submission_date, INTERVAL 1 DAY)
    AND client_info.client_id IS NOT NULL
),
metrics_ping AS (
  SELECT
    client_id,
    sample_id,
    ARRAY_AGG(
      IF(install_source IS NOT NULL, install_source, NULL) IGNORE NULLS
      ORDER BY
        submission_timestamp ASC
      LIMIT
        1
    )[SAFE_OFFSET(0)] AS install_source,
  FROM
    metrics_ping_base
  GROUP BY
    client_id,
    sample_id
)
SELECT
  @submission_date AS submission_date,
  client_id,
  sample_id,
  metrics_ping.install_source,
FROM
  new_profiles
LEFT JOIN
  metrics_ping
  USING (client_id, sample_id)
