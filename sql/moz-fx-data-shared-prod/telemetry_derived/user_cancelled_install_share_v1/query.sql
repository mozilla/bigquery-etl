SELECT
  DATE(submission_timestamp) AS submission_date,
  installer_type,
  update_channel,
  COUNT(1) AS nbr_install_attempts,
  ROUND(AVG(IF(User_cancelled = TRUE, 1, 0)) * 100, 1) AS user_cancelled,
  ROUND(AVG(IF(Install_timeout = TRUE, 1, 0)) * 100, 1) AS `timeout`,
FROM
  `moz-fx-data-shared-prod.firefox_installer.install`
WHERE
  DATE(submission_timestamp) = @submission_date
GROUP BY
  DATE(submission_timestamp),
  installer_type,
  update_channel
