SELECT
  first_seen_date,
  channel,
  first_reported_country,
  SUM(client_count) AS client_count,
FROM
  `data-observability-dev.fenix.firefox_android_aggregates`
WHERE
  submission_date = @submission_date
GROUP BY
  first_seen_date,
  channel,
  first_reported_country
