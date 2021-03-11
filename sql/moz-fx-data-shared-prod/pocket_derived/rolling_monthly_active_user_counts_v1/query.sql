SELECT
  measured_at,
  user_count
FROM
  rolling_monthly_active_user_counts_history_v1
WHERE
  submission_date = @submission_date
