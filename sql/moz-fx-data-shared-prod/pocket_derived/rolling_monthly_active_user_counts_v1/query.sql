SELECT
  measured_at,
  user_count
FROM
  `moz-fx-data-shared-prod.pocket_derived.rolling_monthly_active_user_counts_history_v1`
WHERE
  submission_date = @submission_date
