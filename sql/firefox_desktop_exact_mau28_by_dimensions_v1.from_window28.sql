-- Alternative method using clients_daily_window28_v1 instead of clients_last_seen_v1
WITH
  _sample AS (
  SELECT
    submission_date,
    -- dimensions
    client_id,
    sample_id,
    days[SAFE_ORDINAL(1)].* EXCEPT (submission_date_s3),
    -- usage criteria by days since submission_date_s3
    ARRAY(
    SELECT
      AS STRUCT DATE_DIFF(submission_date, day.submission_date_s3, DAY) AS _date_diff,
      day.scalar_parent_browser_engagement_total_uri_count_sum >= 5 AS _visited_5_uri
    FROM
      UNNEST(days) AS day) AS _criteria
  FROM
    clients_daily_window28_v1
  WHERE
    submission_date = @submission_date ),
  _reduced AS (
  SELECT
    *,
    -- Dimensions for how many of the last N days usage critera were met
    (SELECT COUNT(*) FROM UNNEST(_criteria) WHERE _visited_5_uri AND _date_diff < 28) AS visited_5_uri_frequency28,
    (SELECT COUNT(*) FROM UNNEST(_criteria) WHERE _visited_5_uri AND _date_diff < 7) AS visited_5_uri_frequency7,
    -- For counting clients that met usage criteria the last N days
    (SELECT MIN(_date_diff) FROM UNNEST(_criteria)) AS _min_date_diff,
    (SELECT MIN(_date_diff) FROM UNNEST(_criteria) WHERE _visited_5_uri) AS _visited_5_uri_min_date_diff
  FROM
    _sample
  )
SELECT
  submission_date,
  COUNTIF(_min_date_diff < 28) AS mau,
  COUNTIF(_min_date_diff < 7) AS wau,
  COUNTIF(_min_date_diff < 1) AS dau,
  -- Active MAU https://docs.telemetry.mozilla.org/cookbooks/active_dau.html
  COUNTIF(_visited_5_uri_min_date_diff < 28) AS visited_5_uri_mau,
  COUNTIF(_visited_5_uri_min_date_diff < 7) AS visited_5_uri_wau,
  COUNTIF(_visited_5_uri_min_date_diff < 1) AS visited_5_uri_dau,
  -- We hash client_ids into 20 buckets to aid in computing
  -- confidence intervals for mau/wau/dau sums; the particular hash
  -- function and number of buckets is subject to change in the future.
  MOD(ABS(FARM_FINGERPRINT(client_id)), 20) AS id_bucket,
  -- Dimensions for how many of the last N days usage critera were met
  visited_5_uri_frequency28,
  visited_5_uri_frequency7,
  -- Requested dimensions from bug 1525689
  attribution.source,
  attribution.medium,
  attribution.campaign,
  attribution.content,
  country,
  distribution_id
FROM
  _reduced
GROUP BY
  submission_date,
  id_bucket,
  visited_5_uri_frequency28,
  visited_5_uri_frequency7,
  source,
  medium,
  campaign,
  content,
  country,
  distribution_id
