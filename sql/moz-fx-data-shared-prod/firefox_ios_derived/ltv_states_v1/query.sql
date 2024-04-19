DECLARE first_cohort_date DATE DEFAULT {{first_cohort_date}};
DECLARE last_cohort_date DATE DEFAULT {{last_cohort_date}};
DECLARE last_observation_date DATE DEFAULT {{last_observation_date}};
DECLARE min_sample_id INT64 DEFAULT {{min_sample_id}};
DECLARE max_sample_id INT64 DEFAULT {{max_sample_id}};
DECLARE max_weeks INT64 DEFAULT {{max_weeks}};
DECLARE death_time INT64 DEFAULT {{death_time}};

WITH base AS (
  SELECT
  client_id,
  sample_id,
  submission_date,
  first_seen_date,
  days_since_first_seen,
  days_since_seen,
  BIT_COUNT(`mozfun`.bytes.extract_bits(days_seen_bytes , -{{lookback}}, {{lookback}})) AS pattern,
  -- there was a spike in 0 duration pings in may of 2023, ignore those   
  IF((durations > 0) AND (BIT_COUNT(`mozfun`.bytes.extract_bits(days_seen_bytes , -1, 1)) = 1), 1, 0) AS active
  FROM mozdata.firefox_ios.baseline_clients_yearly
  WHERE sample_id >= min_sample_id
  AND sample_id <= max_sample_id
  AND submission_date >= first_cohort_date
  AND submission_date <= last_observation_date
  AND first_seen_date >= first_cohort_date -- clients newer than first cohort we care about
  AND first_seen_date <= last_cohort_date -- clients not any newer than that last cohort
  AND submission_date >= first_seen_date
  AND NOT (BIT_COUNT(days_seen_bytes) != 1 AND DATE_DIFF(submission_date, first_seen_date, DAY) = 0)
),

ad_clicks AS (
  SELECT
  b.client_id,
  b.sample_id,
  b.submission_date,
  b.first_seen_date,
  b.days_since_first_seen,
  b.days_since_seen,
  b.pattern,
  b.active,
  attr_clients.ad_clicks AS ad_clicks
  FROM base b
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
ac.pattern,
ac.active,
ac.ad_clicks,
c.adjust_network,
c.first_reported_country,
c.first_reported_isp
FROM ad_clicks ac
JOIN
mozdata.firefox_ios.firefox_ios_clients c
USING (sample_id, client_id)
  