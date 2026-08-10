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
  events.category = "credit_card_key_regeneration"
  AND events.name = "keychain_data_lost"
  AND DATE(submission_timestamp) = @submission_date
GROUP BY
  1,
  2,
  3
