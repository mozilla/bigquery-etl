SELECT
  submission_date,
  COUNTIF(days_since_seen < 28) AS mau,
  COUNTIF(days_since_seen < 7) AS wau,
  COUNTIF(days_since_seen < 1) AS dau,
  -- Active MAU counts all Active Users on any day in the last 28 days not just
  -- the most recent day making COUNTIF(_days_since_seen < 28 AND visited_5_uri)
  -- incorrect. Instead we track days_since_visited_5_uri and use that.
  -- https://docs.telemetry.mozilla.org/cookbooks/active_dau.html
  COUNTIF(days_since_visited_5_uri < 28) AS visited_5_uri_mau,
  COUNTIF(days_since_visited_5_uri < 7) AS visited_5_uri_wau,
  COUNTIF(days_since_visited_5_uri < 1) AS visited_5_uri_dau,
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
  COALESCE(cc.name, cls.country) AS country_name,
  distribution_id
FROM
  telemetry.clients_last_seen_v1 cls
LEFT JOIN
  static.country_codes_v1 cc
  ON (cls.country = cc.code)
WHERE
  client_id IS NOT NULL
  -- Reprocess all dates by running this query with --parameter=submission_date:DATE:NULL
  AND (@submission_date IS NULL OR @submission_date = submission_date)
GROUP BY
  submission_date,
  id_bucket,
  source,
  medium,
  campaign,
  content,
  country,
  country_name,
  distribution_id
