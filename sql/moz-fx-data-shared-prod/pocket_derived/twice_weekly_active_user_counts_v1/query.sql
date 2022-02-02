SELECT
  active_at,
  user_count,
  yearly_cumulative_user_count,
FROM
  twice_weekly_active_user_counts_history_v1
WHERE
  submission_date = @submission_date
