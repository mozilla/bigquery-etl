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
  product,
  normalized_channel,
  campaign,
  country,
  distribution_id
FROM
  telemetry.nondesktop_clients_last_seen
WHERE
  contributes_to_2020_kpi
  -- 2017-01-01 is the first populated day of telemetry_core_parquet, so start 28 days later.
  AND submission_date >= DATE '2017-01-28'
  -- Reprocess all dates by running this query with --parameter=submission_date:DATE:NULL
  AND (@submission_date IS NULL OR @submission_date = submission_date)
GROUP BY
  submission_date,
  id_bucket,
  product,
  normalized_channel,
  campaign,
  country,
  distribution_id
