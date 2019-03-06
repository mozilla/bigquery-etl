SELECT
  submission_date,
  CURRENT_DATETIME() AS generated_time,
  STRUCT ( submission_date AS seen,
    IF(country IN ('US', 'FR', 'DE', 'UK', 'CA'),
      submission_date,
      NULL) AS seen_in_tier1_country ) AS last_date,
  * EXCEPT (submission_date, generated_time)
FROM
  core_clients_daily_v1
WHERE
  -- 2017-01-01 is the first date in telemetry_core_parquet_v3.
  submission_date = DATE('2017-01-01')
