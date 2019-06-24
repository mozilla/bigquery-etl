
--
SELECT
  submission_date,
  COUNTIF(days_since_seen < 28) AS mau,
  COUNTIF(days_since_seen < 7) AS wau,
  COUNTIF(days_since_seen < 1) AS dau,
  -- We hash client_ids into 20 buckets to aid in computing
  -- confidence intervals for mau/wau/dau sums; the particular hash
  -- function and number of buckets is subject to change in the future.
  MOD(ABS(FARM_FINGERPRINT(client_id)), 20) AS id_bucket,
  -- Instead of app_name and os, we provide a single clean "product" name
  -- that includes OS where necessary to disambiguate.
  CASE app_name
    WHEN 'Fennec' THEN CONCAT(app_name, ' ', os)
    WHEN 'Focus' THEN CONCAT(app_name, ' ', os)
    WHEN 'Zerda' THEN 'Firefox Lite'
    ELSE app_name
  END AS product,
  normalized_channel,
  campaign,
  country,
  distribution_id
FROM
  core_clients_last_seen_v1
WHERE
  -- This list corresponds to the products considered for 2019 nondesktop KPIs;
  -- we apply this filter here rather than in the live view because this field
  -- is not normalized and there are many single pings that come in with unique
  -- nonsensical app_name values.
  app_name IN (
    'Fennec', -- Firefox for Android and Firefox for iOS
    'Focus',
    'Zerda', -- Firefox Lite, previously called Rocket
    'FirefoxForFireTV', -- Amazon Fire TV
    'FirefoxConnect' -- Amazon Echo Show
    )
  -- There are also many strange nonsensical entries for os, so we filter here.
  AND os IN ('Android', 'iOS')
  -- 2017-01-01 is the first populated day of telemetry_core_parquet, so start 28 days later.
  AND submission_date >= DATE '2017-01-28'
  AND submission_date = @submission_date
GROUP BY
  submission_date,
  id_bucket,
  product,
  normalized_channel,
  campaign,
  country,
  distribution_id
