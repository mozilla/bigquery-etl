SELECT
  client_id,
  sample_id,
  submission_date,
  first_seen_date,
  days_since_first_seen,
  days_since_seen,
  --? AS death_time,
  BIT_COUNT(`mozfun`.bytes.extract_bits(days_seen_bytes , -{{lookback}}, {{lookback}})) AS pattern, --what to use for lookback?
  IF((durations > 0) AND (BIT_COUNT(`mozfun`.bytes.extract_bits(days_seen_bytes , -1, 1)) = 1), 1, 0) AS active,
  ? AS death_time,
  --? AS max_weeks
FROM
  `mozdata.firefox_ios.baseline_clients_yearly`
WHERE
  submission_date = @submission_date
