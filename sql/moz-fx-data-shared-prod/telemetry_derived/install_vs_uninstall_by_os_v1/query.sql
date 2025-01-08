WITH installs AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    CASE
      WHEN os_version LIKE "10%"
        THEN '10.0'
      WHEN os_version LIKE "6.1%"
        THEN '6.1'
      WHEN os_version LIKE "6.2%"
        THEN '6.2'
      WHEN os_version LIKE "6.3%"
        THEN '6.3'
      ELSE 'other'
    END AS os_version_bucket,
    COUNT(*) AS stub_release_installs_count,
  FROM
    `moz-fx-data-shared-prod.firefox_installer.install`
  WHERE
    update_channel = 'release'
    AND installer_type = 'stub'
    AND DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    os_version_bucket
),
uninstalls AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    normalized_os_version,
    COUNT(DISTINCT client_id) AS uninstalls_count
  FROM
    `moz-fx-data-shared-prod.telemetry.uninstall`
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND application.name = 'Firefox'
    AND application.channel = 'release'
    AND normalized_os_version IN ('6.1', '6.2', '6.3', '10.0')
  GROUP BY
    submission_date,
    normalized_os_version
)
SELECT
  i.submission_date,
  u.normalized_os_version,
  SAFE_DIVIDE(u.uninstalls_count, i.stub_release_installs_count) AS ratio_uninstalls_to_installs,
FROM
  installs AS i
JOIN
  uninstalls AS u
  ON i.submission_date = u.submission_date
  AND i.os_version_bucket = u.normalized_os_version
