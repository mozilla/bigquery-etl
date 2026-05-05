WITH installs AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    normalized_country_code,
    COUNT(*) AS installs_count
  FROM
    `moz-fx-data-shared-prod.firefox_installer.install`
  WHERE
    update_channel = 'release'
    AND installer_type = 'stub'
    AND DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    normalized_country_code
),
uninstalls AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    normalized_country_code,
    COUNT(DISTINCT client_id) AS uninstalls_count,
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
  COALESCE(a.submission_date, b.submission_date) AS submission_date,
  COALESCE(a.normalized_country_code, b.normalized_country_code) AS normalized_country_code,
  COALESCE(b.uninstalls_count, 0) AS uninstalls_count,
  COALESCE(a.installs_count, 0) AS installs_count,
  SAFE_DIVIDE(
    COALESCE(b.uninstalls_count, 0),
    COALESCE(a.installs_count, 0)
  ) AS uninstall_to_install_ratio
FROM
  installs a
FULL OUTER JOIN
  uninstalls AS b
  ON a.normalized_country_code = b.normalized_country_code
  AND a.submission_date = b.submission_date
