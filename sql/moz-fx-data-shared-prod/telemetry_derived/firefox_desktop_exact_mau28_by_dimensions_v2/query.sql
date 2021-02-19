SELECT
  submission_date,
  COUNTIF(days_since_seen < 28) AS mau,
  COUNTIF(days_since_seen < 7) AS wau,
  COUNTIF(days_since_seen < 1) AS dau,
  COUNTIF(first_seen_date = submission_date) AS new_profiles,
  -- We hash client_ids into 20 buckets to aid in computing
  -- confidence intervals for mau/wau/dau sums; the particular hash
  -- function and number of buckets is subject to change in the future.
  MOD(ABS(FARM_FINGERPRINT(client_id)), 20) AS id_bucket,
  activity_segments_v1 AS activity_segment,
  mozfun.norm.os(os) AS os,
  normalized_channel AS channel,
  -- requested fields from bug 1525689
  attribution.source,
  attribution.medium,
  attribution.campaign,
  attribution.content,
  country,
  distribution_id
FROM
  telemetry.clients_last_seen
WHERE
  client_id IS NOT NULL
  -- Reprocess all dates by running this query with --parameter=submission_date:DATE:NULL
  AND (@submission_date IS NULL OR @submission_date = submission_date)
GROUP BY
  submission_date,
  id_bucket,
  activity_segment,
  os,
  normalized_channel,
  source,
  medium,
  campaign,
  content,
  country,
  distribution_id
