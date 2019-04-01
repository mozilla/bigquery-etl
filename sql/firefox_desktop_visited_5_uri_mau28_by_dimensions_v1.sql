WITH
  _sample AS (
  SELECT
    submission_date,
    -- most recent values for inclusion as output dimensions
    days[SAFE_ORDINAL(1)] AS last_seen,
    DATE_DIFF(submission_date, (
      SELECT
        -- bit-packed boolean array of days meeting the where condition
        -- with most significant bit as the least recent day
        SUM(1 << DATE_DIFF(submission_date, submission_date_s3, DAY))
      FROM
        UNNEST(days)
      WHERE
        -- results in NULL AS _days_visited_5_uri when condition not met by any item in days
        scalar_parent_browser_engagement_total_uri_count_sum >= 5), DAY) AS _days_visited_5_uri
  FROM
    clients_monthly_v1
  WHERE
    submission_date = @submission_date )
SELECT
  submission_date,
  last_seen.attribution.source,
  last_seen.attribution.medium,
  last_seen.attribution.campaign,
  last_seen.attribution.content,
  last_seen.country,
  last_seen.distribution_id,
  -- BIT_COUNT returns the number of set bits
  -- 0x0FFFFFFF is a bit-mask for the lowest 28 bits
  -- 0x7F is a bit-mask for the lowest 7 bits
  -- Count the number of days in the window that a client visited 5 uri
  BIT_COUNT(_days_visited_5_uri & 0x0FFFFFFF) AS visited_5_uri_frequency28,
  BIT_COUNT(_days_visited_5_uri & 0x7F) AS visited_5_uri_frequency7,
  -- Count unique clients that visited 5 uri on any day in the window
  COUNTIF(_days_visited_5_uri & 0x0FFFFFFF > 0) AS visited_5_uri_mau,
  COUNTIF(_days_visited_5_uri & 0x7F > 0) AS visited_5_uri_wau,
  COUNTIF(_days_visited_5_uri & 1 > 0) AS visited_5_uri_dau
FROM
  _sample
WHERE
  _days_visted_5_uri IS NOT NULL
GROUP BY
  submission_date,
  source,
  medium,
  campaign,
  content,
  country,
  distribution_id,
  visited_5_uri_frequency28,
  visited_5_uri_frequency7
