SELECT
  DATE(submission_timestamp) AS submission_date,
  "firefox_android" AS application,
  normalized_channel AS channel,
  dual_labeled_counter.key AS label1,
  dual_labeled_counter2.key AS label2,
  SUM(dual_labeled_counter2.value) AS count,
  COUNT(DISTINCT client_info.client_id) AS client_count
FROM
  `moz-fx-data-shared-prod.fenix.metrics`
JOIN
  UNNEST(
    metrics.dual_labeled_counter.rust_component_errors_expected_failures
  ) AS dual_labeled_counter
JOIN
  UNNEST(dual_labeled_counter.value) AS dual_labeled_counter2
WHERE
  DATE(submission_timestamp) = @submission_date
GROUP BY
  submission_date,
  application,
  channel,
  label1,
  label2
UNION ALL
SELECT
  DATE(submission_timestamp) AS submission_date,
  "firefox_ios" AS application,
  normalized_channel AS channel,
  dual_labeled_counter.key AS label1,
  dual_labeled_counter2.key AS label2,
  SUM(dual_labeled_counter2.value) AS count,
  COUNT(DISTINCT client_info.client_id) AS client_count
FROM
  `moz-fx-data-shared-prod.firefox_ios.metrics`
JOIN
  UNNEST(
    metrics.dual_labeled_counter.rust_component_errors_expected_failures
  ) AS dual_labeled_counter
JOIN
  UNNEST(dual_labeled_counter.value) AS dual_labeled_counter2
WHERE
  DATE(submission_timestamp) = @submission_date
GROUP BY
  submission_date,
  application,
  channel,
  label1,
  label2
UNION ALL
SELECT
  DATE(submission_timestamp) AS submission_date,
  "firefox_desktop" AS application,
  normalized_channel AS channel,
  dual_labeled_counter.key AS label1,
  dual_labeled_counter2.key AS label2,
  SUM(dual_labeled_counter2.value) AS count,
  COUNT(DISTINCT client_info.client_id) AS client_count
FROM
  `moz-fx-data-shared-prod.firefox_desktop.metrics`
JOIN
  UNNEST(
    metrics.dual_labeled_counter.rust_component_errors_expected_failures
  ) AS dual_labeled_counter
JOIN
  UNNEST(dual_labeled_counter.value) AS dual_labeled_counter2
WHERE
  DATE(submission_timestamp) = @submission_date
GROUP BY
  submission_date,
  application,
  channel,
  label1,
  label2
