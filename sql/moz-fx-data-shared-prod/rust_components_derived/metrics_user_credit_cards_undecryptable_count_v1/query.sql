SELECT
  DATE(submission_timestamp) AS submission_date,
  "firefox_ios" AS application,
  normalized_channel AS channel,
  SUM(metrics.counter.user_credit_cards_undecryptable_count) AS count,
  COUNT(DISTINCT client_info.client_id) AS client_count
FROM
  `moz-fx-data-shared-prod.firefox_ios.metrics`
WHERE
  DATE(submission_timestamp) = @submission_date
  AND metrics.counter.user_credit_cards_undecryptable_count IS NOT NULL
GROUP BY
  1,
  2,
  3
