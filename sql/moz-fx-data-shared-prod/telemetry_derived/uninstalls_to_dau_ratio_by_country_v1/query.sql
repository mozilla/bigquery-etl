WITH daily_active_users AS (
  SELECT
    submission_date,
    country,
    COUNT(DISTINCT client_id) AS total_dau
  FROM
    `moz-fx-data-shared-prod.telemetry.clients_daily`
  WHERE
    submission_date = @submission_date
    AND COALESCE(country, '??') <> '??'
  GROUP BY
    submission_date,
    country
),
uninstalls AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    normalized_country_code,
    COUNT(DISTINCT client_id) AS nbr_clients_uninstalling
  FROM
    `moz-fx-data-shared-prod.telemetry.uninstall`
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND application.name = 'Firefox'
    AND application.channel = 'release'
  GROUP BY
    submission_date,
    normalized_country_code
)
SELECT
  dau.submission_date,
  dau.country AS normalized_country_code,
  dau.total_dau,
  u.nbr_clients_uninstalling,
  SAFE_DIVIDE(u.nbr_clients_uninstalling, dau.total_dau) AS ratio_uninstalls_to_dau,
FROM
  daily_active_users AS dau
LEFT JOIN
  uninstalls AS u
  ON dau.country = u.normalized_country_code
  AND dau.submission_date = u.submission_date
