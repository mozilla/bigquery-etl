SELECT
  first_seen_date,
  channel,
  first_reported_country,
  install_source,
  distribution_id,
  COUNT(*) AS client_count,
FROM
  `data-observability-dev.fenix.firefox_android_clients`
WHERE
  submission_date = @submission_date
GROUP BY
  first_seen_date,
  channel,
  first_reported_country,
  install_source,
  distribution_id
