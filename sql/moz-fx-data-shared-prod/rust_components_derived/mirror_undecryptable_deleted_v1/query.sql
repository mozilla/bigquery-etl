SELECT
  DATE(submission_timestamp) AS submission_date,
  "firefox_android" AS application,
  SUM(metrics.counter.logins_store_mirror_undecryptable_deleted) AS count
FROM
  `mozdata.fenix.metrics`
WHERE
  DATE(submission_timestamp) = @submission_date
  AND metrics.counter.logins_store_mirror_undecryptable_deleted IS NOT NULL
GROUP BY
  1,
  2
UNION ALL
SELECT
  DATE(submission_timestamp) AS submission_date,
  "firefox_ios" AS application,
  SUM(metrics.counter.logins_store_mirror_undecryptable_deleted) AS count
FROM
  `mozdata.firefox_ios.metrics`
WHERE
  DATE(submission_timestamp) = @submission_date
  AND metrics.counter.logins_store_mirror_undecryptable_deleted IS NOT NULL
GROUP BY
  1,
  2
