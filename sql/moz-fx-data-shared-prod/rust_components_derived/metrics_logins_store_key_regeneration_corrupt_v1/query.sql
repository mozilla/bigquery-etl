SELECT
  DATE(submission_timestamp) AS submission_date,
  "firefox_ios" AS application,
  normalized_channel AS channel,
  COUNT(*) AS count,
  COUNT(DISTINCT client_info.client_id) AS client_count
FROM
  `moz-fx-data-shared-prod.firefox_ios.events`
CROSS JOIN
  UNNEST(events) AS events
WHERE
  events.category = "logins_store_key_regeneration"
  AND events.name = "corrupt"
  AND DATE(submission_timestamp) = @submission_date
GROUP BY
  1,
  2,
  3
