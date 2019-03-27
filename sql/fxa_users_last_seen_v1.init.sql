SELECT
  submission_date,
  CURRENT_DATETIME() AS generated_time,
  submission_date AS date_last_seen,
  IF(country IN ('United States', 'France', 'Germany', 'United Kingdom', 'Canada'),
    submission_date,
    NULL) AS date_last_seen_in_tier1_country,
  * EXCEPT (submission_date, generated_time, seen_in_tier1_country)
FROM
  fxa_users_daily_v1
WHERE
  -- Choose an arbitrary date that contains no records.
  submission_date = DATE_FROM_UNIX_DATE(0)
