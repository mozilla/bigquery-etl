WITH base AS (
  SELECT
    client_id,
    sample_id,
    submission_date,
    first_seen_date,
    days_since_first_seen,
    days_since_seen,
    days_seen_bytes,
    durations
  FROM
    mozdata.firefox_ios.baseline_clients_yearly
  WHERE
    submission_date = @submission_date
    AND NOT (
      BIT_COUNT(days_seen_bytes) != 1
      AND DATE_DIFF(submission_date, first_seen_date, DAY) = 0
    )
),
ad_clicks AS (
  SELECT
    b.client_id,
    b.sample_id,
    b.submission_date,
    b.first_seen_date,
    b.days_since_first_seen,
    b.days_since_seen,
    b.days_seen_bytes,
    b.durations,
    attr_clients.ad_clicks AS ad_clicks
  FROM
    base b
  LEFT JOIN
    mozdata.firefox_ios.attributable_clients attr_clients
    ON b.client_id = attr_clients.client_id
    AND b.submission_date = attr_clients.submission_date
)
SELECT
  ac.client_id,
  ac.sample_id,
  ac.submission_date,
  ac.first_seen_date,
  ac.days_since_first_seen,
  ac.days_since_seen,
  ac.days_seen_bytes,
  ac.durations,
  ac.ad_clicks,
  c.adjust_network,
  c.first_reported_country,
  c.first_reported_isp
FROM
  ad_clicks ac
JOIN
  mozdata.firefox_ios.firefox_ios_clients c
  USING (sample_id, client_id)
