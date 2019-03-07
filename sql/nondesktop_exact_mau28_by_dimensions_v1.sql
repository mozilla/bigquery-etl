WITH
  inactive_days AS (
    SELECT
      *,
      DATE_DIFF(submission_date, date_last_seen, DAY) AS _inactive_days,
      DATE_DIFF(submission_date, date_last_seen_in_tier1_country, DAY) AS _inactive_days_tier1
    FROM
      core_clients_last_seen_v1
  )

SELECT
  submission_date,
  CURRENT_DATETIME() AS generated_time,
  COUNTIF(_inactive_days < 28) AS mau,
  COUNTIF(_inactive_days < 7) AS wau,
  COUNTIF(_inactive_days < 1) AS dau,
  -- You can recover MAU for all clients in tier 1 countries via a WHERE clause
  -- and summing mau, but this considers only the country associated with the
  -- latest observation of the client; "Tier 1 grouped" MAU is an alternative
  -- that includes all clients seen in a tier 1 country in the 28 day window,
  -- even if their latest ping came from an excluded country.
  COUNTIF(_inactive_days_tier1 < 28) AS tier1_grouped_mau,
  COUNTIF(_inactive_days_tier1 < 7) AS tier1_grouped_wau,
  COUNTIF(_inactive_days_tier1 < 1) AS tier1_grouped_dau,
  -- We filter down to a specific set of applications considered for 2019 goals
  -- and provide a single clean "product" name that includes OS where necessary.
  CASE app_name
    WHEN 'Fennec' THEN CONCAT(app_name, ' ', os)
    WHEN 'Focus' THEN CONCAT(app_name, ' ', os)
    WHEN 'Zerda' THEN 'Firefox Lite'
    ELSE app_name
  END AS product,
  campaign,
  country,
  distribution_id
FROM
  inactive_days
WHERE
  app_name IN ('Fennec', 'Focus', 'Zerda', 'FirefoxForFireTV', 'FirefoxConnect')
  AND os IN ('Android', 'iOS')
  -- 2017-01-01 is the first populated day of telemetry_core_parquet, so start 28 days later.
  AND submission_date >= '2017-01-28'
  AND submission_date = @submission_date
GROUP BY
  submission_date,
  product,
  campaign,
  country,
  distribution_id
