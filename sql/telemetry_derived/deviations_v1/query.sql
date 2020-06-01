SELECT
  date,
  ci_deviation,
  deviation,
  geography,
  metric
FROM
  deviations_anomdtct_v1
WHERE
  -- We explicitly enumerate allowed metric types here so that we do not automatically
  -- publish new metric types publicly without review.
  metric IN ('desktop_dau', 'mean_active_hours_per_client')
  AND date = @submission_date
