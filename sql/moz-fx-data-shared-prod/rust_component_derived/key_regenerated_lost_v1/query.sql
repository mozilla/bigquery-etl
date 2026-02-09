SELECT
  DATE(submission_timestamp) AS submission_date,
  "firefox_android" AS application,
  COUNT(*) AS count
FROM
  `mozdata.fenix.events`
CROSS JOIN
  UNNEST(events) AS events
WHERE
  events.category = "logins_store"
  AND events.name = "key_regenerated_lost"
  AND DATE(submission_timestamp) = @submission_date
GROUP BY
  1
UNION ALL
SELECT
  DATE(submission_timestamp) AS submission_date,
  "firefox_ios" AS application,
  COUNT(*) AS count
FROM
  `mozdata.firefox_ios.events`
CROSS JOIN
  UNNEST(events) AS events
WHERE
  events.category = "logins_store"
  AND events.name = "key_regenerated_lost"
  AND DATE(submission_timestamp) = @submission_date
GROUP BY
  1
