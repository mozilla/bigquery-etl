SELECT
  DATE(submission_timestamp) AS submission_date,
  CASE
    WHEN country IN ('US', 'CA', 'FR', 'DE', 'GB')
      THEN country
    ELSE 'OTHER'
  END AS country,
  100 * COUNT(DISTINCT m.client_info.client_id) AS clients,
  100 * SUM(p.value) AS fill_counts
FROM
  `moz-fx-data-shared-prod.firefox_desktop.metrics` AS m
CROSS JOIN
  UNNEST(metrics.labeled_counter.pwmgr_form_autofill_result) AS p
WHERE
  metrics.labeled_counter.pwmgr_form_autofill_result IS NOT NULL
  AND p.key IN ("filled", "filled_username_only_form")
  AND normalized_channel = 'release'
  AND normalized_app_name = 'Firefox'
  AND DATE(submission_timestamp) = @submission_date
  AND sample_id = 0
GROUP BY
  submission_date,
  country
