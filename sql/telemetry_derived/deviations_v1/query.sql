SELECT
  date,
  ci_deviation,
  deviation,
  geography,
  metric
FROM
  `moz-fx-data-shared-prod.analysis.deviations`
WHERE date = @submission_date
