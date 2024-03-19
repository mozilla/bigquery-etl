SELECT
  *
FROM
  `moz-fx-data-shared-prod.fenix_derived.events_daily_v1`
WHERE
  submission_date = @submission_date
