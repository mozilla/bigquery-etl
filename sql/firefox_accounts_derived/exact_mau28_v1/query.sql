WITH
  -- We have a one-time backfill of FxA data extracted from Amplitude that
  -- runs through EOD 2019-03-27.
backfill AS (
  SELECT
    submission_date,
    user_id,
    DATE_DIFF(submission_date, date_last_seen, DAY) AS days_since_seen,
    DATE_DIFF(
      submission_date,
      date_last_seen_in_tier1_country,
      DAY
    ) AS days_since_seen_in_tier1_country,
    country
  FROM
    static.fxa_amplitude_export_users_last_seen
  WHERE
    submission_date < DATE '2019-03-28'
),
  -- "Live" FxA data gets to BigQuery via a Stackdriver pipeline that had its
  -- first stable day on 2019-03-01, so first valid day for MAU is 2019-03-28.
live AS (
  SELECT
    submission_date,
    user_id,
    days_since_seen,
    days_since_seen_in_tier1_country,
    country
  FROM
    firefox_accounts.fxa_users_last_seen
  WHERE
    submission_date >= DATE '2019-03-28'
),
unioned AS (
  SELECT
    *
  FROM
    backfill
  UNION ALL
  SELECT
    *
  FROM
    live
)
  --
SELECT
  submission_date,
  COUNTIF(days_since_seen < 28) AS mau,
  COUNTIF(days_since_seen < 7) AS wau,
  COUNTIF(days_since_seen < 1) AS dau,
  -- We are generally using an "exclusive dimensions" methodology where only
  -- the last country observed for a user is considered for determining whether
  -- they contribute to tier 1 MAU, but we also include an "inclusive" tier 1
  -- mau calculation that we have previously been using for KPI calculations on
  -- FxA data; we assign a single country per user per day and include a user
  -- in this calculation if they were assigned a tier 1 country in any of the
  -- 28 days of the MAU window.
  COUNTIF(days_since_seen_in_tier1_country < 28) AS mau_tier1_inclusive,
  -- We hash user_ids into 20 buckets to aid in computing
  -- confidence intervals for mau/wau/dau sums; the particular hash
  -- function and number of buckets is subject to change in the future.
  MOD(ABS(FARM_FINGERPRINT(user_id)), 20) AS id_bucket,
  country
FROM
  unioned
WHERE
  -- First data is on 2017-10-01, so we start 28 days later for first complete MAU value.
  submission_date >= DATE '2017-10-28'
  -- Reprocess all dates by running this query with --parameter=submission_date:DATE:NULL
  AND (@submission_date IS NULL OR @submission_date = submission_date)
GROUP BY
  submission_date,
  id_bucket,
  country
