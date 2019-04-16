SELECT
  submission_date,
  submission_date AS date_last_seen,
  IF(country IN ('US', 'FR', 'DE', 'UK', 'CA'),
    submission_date,
    NULL) AS date_last_seen_in_tier1_country,
  * EXCEPT (submission_date)
FROM
  core_clients_daily_v1
WHERE
  -- Output empty table and read no input rows
  FALSE
