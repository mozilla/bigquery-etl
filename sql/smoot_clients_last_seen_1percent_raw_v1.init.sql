CREATE
OR REPLACE
TABLE
  smoot_clients_last_seen_1percent_raw_v1
PARTITION BY
  submission_date AS
SELECT
  DATE(NULL) AS submission_date,
  CURRENT_DATETIME() AS generated_time,
  * EXCEPT (submission_date_s3),
  0 AS days_seen_bits,
  0 AS days_visited_5_uri_bits,
  0 AS days_opened_dev_tools_bits,
  0 AS days_since_created_profile
FROM
  smoot_clients_daily_1percent_v1
WHERE
  -- Output empty table and read no input rows
  FALSE
