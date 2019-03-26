WITH
  inactive_days AS (
    SELECT
      *,
      DATE_DIFF(submission_date, date_last_seen, DAY) AS _inactive_days,
      DATE_DIFF(submission_date, date_last_seen_in_tier1_country, DAY) AS _inactive_days_tier1
    FROM
      fxa_users_last_seen_v1
  )

SELECT
  submission_date,
  CURRENT_DATETIME() AS generated_time,
  COUNTIF(_inactive_days < 28) AS mau,
  COUNTIF(_inactive_days < 7) AS wau,
  COUNTIF(_inactive_days < 1) AS dau,
  -- We are generally using an "exclusive dimensions" methodology where only
  -- the last country observed for a user is considered for determining whether
  -- they contribute to tier 1 MAU, but we also include an "inclusive" tier 1
  -- mau calculation that we have previously been using for KPI calculations on
  -- FxA data; we assign a single country per user per day and include a user
  -- in this calculation if they were assigned a tier 1 country in any of the
  -- 28 days of the MAU window.
  COUNTIF(_inactive_days_tier1 < 28) AS mau_tier1_inclusive,
  -- We hash user_ids into 20 buckets to aid in computing
  -- confidence intervals for mau/wau/dau sums; the particular hash
  -- function and number of buckets is subject to change in the future.
  MOD(ABS(FARM_FINGERPRINT(user_id)), 20) AS id_bucket,
  country
FROM
  inactive_days
WHERE
  -- First data is on 2017-10-01, so we start 28 days later for first complete MAU value.
  submission_date >= DATE '2017-10-28'
  AND submission_date = @submission_date
GROUP BY
  submission_date,
  id_bucket,
  country
