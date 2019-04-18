SELECT
  DATE(NULL) AS submission_date,
  submission_date_s3 AS last_seen_date,
  * EXCEPT (submission_date_s3),
  0 AS days_since_seen,
  NULL AS days_since_visited_5_uri
FROM
  clients_daily_v6
WHERE
  -- Output empty table and read no input rows
  FALSE
