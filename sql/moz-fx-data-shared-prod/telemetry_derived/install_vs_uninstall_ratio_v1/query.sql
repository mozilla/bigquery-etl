WITH installs AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    COUNT(1) AS release_channel_stub_installs_count
  FROM
    `moz-fx-data-shared-prod.firefox_installer.install`
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND update_channel = 'release'
    AND installer_type = 'stub'
  GROUP BY
    DATE(submission_timestamp)
),
uninstalls AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    COUNT(DISTINCT client_id) AS uninstalls_count,
  FROM
    `moz-fx-data-shared-prod.telemetry.uninstall`
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND application.name = 'Firefox'
    AND application.channel = 'release'
    AND normalized_country_code IN (
      'BR',
      'ID',
      'DE',
      'US',
      'IN',
      'FR',
      'PL',
      'ES',
      'CN',
      'IT',
      'MX',
      'GB',
      'TR',
      'JP',
      'CO',
      'RU',
      'CA',
      'TH',
      'EG',
      'AU'
    )
  GROUP BY
    DATE(submission_timestamp)
)
SELECT
  COALESCE(i.submission_date, u.submission_date) AS submission_date,
  i.release_channel_stub_installs_count,
  u.uninstalls_count,
  SAFE_DIVIDE(
    u.uninstalls_count,
    i.release_channel_stub_installs_count
  ) AS uninstall_to_install_ratio
FROM
  installs i
FULL OUTER JOIN
  uninstalls u
  ON i.submission_date = u.submission_date
