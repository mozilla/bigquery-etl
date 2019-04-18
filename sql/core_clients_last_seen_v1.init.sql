SELECT
  submission_date,
  CAST(NULL AS DATE) AS date_last_seen,
  CAST(NULL AS DATE) AS date_last_seen_in_tier1_country,
  * EXCEPT (submission_date)
FROM
  core_clients_daily_v1
WHERE
  -- Output empty table and read no input rows
  FALSE
