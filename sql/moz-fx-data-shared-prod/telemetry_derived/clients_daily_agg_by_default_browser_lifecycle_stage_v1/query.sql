SELECT
  submission_date,
  is_default_browser,
  normalized_os_version,
  os,
  normalized_channel,
  country,
  CASE
    WHEN first_seen_date = submission_date
      THEN 'new_profile'
    WHEN DATE_DIFF(submission_date, first_seen_date, DAY)
      BETWEEN 1
      AND 27
      THEN 'repeat_user'
    WHEN DATE_DIFF(submission_date, first_seen_date, DAY) >= 28
      THEN 'existing_user'
    ELSE 'Unknown'
  END AS lifecycle_stage,
  COUNT(*) AS row_count,
FROM
  `moz-fx-data-shared-prod.telemetry.clients_daily`
WHERE
  submission_date = @submission_date
GROUP BY
  submission_date,
  is_default_browser,
  os,
  normalized_os_version,
  normalized_channel,
  country,
  lifecycle_stage
