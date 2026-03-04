SELECT
  DATE(submission_timestamp) AS submission_date,
  "firefox_android" AS application,
  normalized_channel AS channel,
  COUNT(*) AS count
FROM
  `moz-fx-data-shared-prod.fenix.events`
CROSS JOIN
  UNNEST(events) AS events
WHERE
  events.category = "logins_store"
  AND events.name = "key_regenerated_lost"
  AND DATE(submission_timestamp) = @submission_date
GROUP BY
  1,
  2,
  3
UNION ALL
SELECT
  DATE(submission_timestamp) AS submission_date,
  "firefox_ios" AS application,
  normalized_channel AS channel,
  COUNT(*) AS count
FROM
  `moz-fx-data-shared-prod.firefox_ios.events`
CROSS JOIN
  UNNEST(events) AS events
WHERE
  events.category = "logins_store"
  AND events.name = "key_regenerated_lost"
  AND DATE(submission_timestamp) = @submission_date
GROUP BY
  1,
  2,
  3
