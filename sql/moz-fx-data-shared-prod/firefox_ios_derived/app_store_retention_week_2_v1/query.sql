WITH clients_retention AS (
  SELECT
    submission_date,
    client_id,
    sample_id,
    days_seen_bits,
  FROM
    firefox_ios.baseline_clients_last_seen
  WHERE
    submission_date = @submission_date
    AND normalized_channel = "release"
),
first_seen AS (
  SELECT
    client_id,
    sample_id,
    first_seen_date,
  FROM
    firefox_ios.baseline_clients_first_seen
  -- Two weeks need to elapse before calculating the week 2 retention
  WHERE
    submission_date = DATE_SUB(@submission_date, INTERVAL 14 DAY)
)
SELECT
  first_seen.first_seen_date,
  clients_retention.client_id,
  clients_retention.sample_id,
  mozfun.bits28.active_in_range(days_seen_bits, -6, 7) AS retained_week_2,
FROM
  clients_retention
INNER JOIN
  first_seen
USING
  (client_id, sample_id)
