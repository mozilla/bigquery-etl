SELECT
  client_id,
  sample_id,
  submission_date,
  first_seen_date,
  days_since_first_seen,
  days_since_seen,
  sample_id,
  isp,
  days_seen_bytes,
  IF((durations > 0) AND (BIT_COUNT(`mozfun`.bytes.extract_bits(days_seen_bytes , -1, 1)) = 1), 1, 0) AS active
FROM
  `mozdata.firefox_ios.baseline_clients_yearly`
WHERE
  submission_date = @submission_date
