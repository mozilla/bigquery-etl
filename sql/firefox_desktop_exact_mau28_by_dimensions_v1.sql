WITH
  inactive_days AS (
    SELECT
      *,
      DATE_DIFF(submission_date, last_seen_date, DAY) AS _inactive_days
    FROM
      clients_last_seen_v1
  )

SELECT
  submission_date,
  CURRENT_DATETIME() AS generated_time,
  COUNTIF(_inactive_days < 28) AS mau,
  COUNTIF(_inactive_days < 7) AS wau,
  COUNTIF(_inactive_days < 1) AS dau,
  -- We hash client_ids into 20 buckets to aid in computing
  -- confidence intervals for mau/wau/dau sums; the particular hash
  -- function and number of buckets is subject to change in the future.
  MOD(ABS(FARM_FINGERPRINT(client_id)), 20) AS id_bucket,
  -- requested fields from bug 1525689
  attribution.source,
  attribution.medium,
  attribution.campaign,
  attribution.content,
  country,
  distribution_id
FROM
  inactive_days
WHERE
  client_id IS NOT NULL
  AND submission_date = @submission_date
GROUP BY
  submission_date,
  id_bucket,
  source,
  medium,
  campaign,
  content,
  country,
  distribution_id
