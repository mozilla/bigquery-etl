SELECT
  DATE(submission_timestamp) AS submission_date,
  normalized_country_code,
  normalized_channel,
  COUNT(DISTINCT client_info.client_id) AS nbr_distinct_clients
FROM
  `moz-fx-data-shared-prod.firefox_desktop_stable.metrics_v1`
CROSS JOIN
  UNNEST(metrics.labeled_counter.browser_ui_interaction_nav_bar) AS a
WHERE
  DATE(submission_timestamp) = @submission_date
  AND LOWER(a.key) LIKE '%reload%'
GROUP BY
  DATE(submission_timestamp),
  normalized_country_code,
  normalized_channel
