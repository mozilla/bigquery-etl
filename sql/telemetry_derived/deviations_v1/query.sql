SELECT
  date,
  ci_deviation,
  deviation,
  geography,
  metric
FROM
  `moz-fx-data-shared-prod.analysis.deviations`
WHERE
  metric IN ('desktop_dau', 'mean_active_hours_per_client')
  AND date = @submission_date
