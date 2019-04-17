SELECT
  submission_date,
  CURRENT_DATETIME() AS generated_time,
  CAST(NULL AS DATE) AS date_last_seen,
  CAST(NULL AS DATE) AS date_last_seen_in_tier1_country,
  * EXCEPT (submission_date, generated_time)
FROM
  core_clients_daily_v1
WHERE
  -- Output empty table and read no input rows
  FALSE
