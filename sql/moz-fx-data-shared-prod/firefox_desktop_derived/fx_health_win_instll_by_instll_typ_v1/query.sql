/*
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
*/
--bad, not working yet: 
  SELECT
    client_id,
    --should I use normalized_channel or app_channel?
    REGEXP_REPLACE(`event`, 'installation.first_seen_', "") AS installer_type,
    ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY submission_timestamp ASC) AS rnk
  FROM
    `moz-fx-data-shared-prod.firefox_desktop.events_stream`
  WHERE
    event_category = 'installation'
    AND sample_id = 0
    AND DATE(submission_timestamp) < @submission_date
  QUALIFY
    rnk = 1
  