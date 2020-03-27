SELECT
  date,
  ci_deviation,
  deviation,
  geography,
  metric
FROM
  `moz-fx-data-derived-datasets.analysis.deviations`
WHERE
  date = @submission_date
