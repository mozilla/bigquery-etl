SELECT
  CAST(submission_timestamp AS DATE) AS submission_date,
  update_channel,
  installer_type,
  COUNT(1) AS install_count,
FROM
  `moz-fx-data-shared-prod.firefox_installer.install`
WHERE
  DATE(submission_timestamp) = @submission_date
GROUP BY
  CAST(submission_timestamp AS DATE),
  update_channel,
  installer_type
