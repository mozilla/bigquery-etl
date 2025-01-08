SELECT
  submission_date,
  COUNTIF(is_default_browser) total_default,
  COUNTIF(is_default_browser AND days_since_first_seen < 28) default_new,
  COUNTIF(is_default_browser AND days_since_first_seen >= 28) default_old,
  COUNT(*) dau,
  SAFE_DIVIDE(
    COUNTIF(is_default_browser AND days_since_first_seen < 28),
    COUNTIF(days_since_first_seen < 28)
  ) share_new_default,
  SAFE_DIVIDE(
    COUNTIF(is_default_browser AND days_since_first_seen >= 28),
    COUNTIF(days_since_first_seen >= 28)
  ) share_old_default
FROM
  `moz-fx-data-shared-prod.telemetry.clients_last_seen`
WHERE
  sample_id = 53
  AND days_since_seen = 0
  AND submission_date = @submission_date
  AND LEFT(normalized_os_version, 2) = '10'
  AND os LIKE '%Win%'
  AND normalized_channel = 'release'
GROUP BY
  submission_date
