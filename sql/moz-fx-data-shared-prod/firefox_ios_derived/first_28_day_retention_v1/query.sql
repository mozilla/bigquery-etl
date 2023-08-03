WITH first_28_day_activity AS (
  SELECT
    client_id,
    sample_id,
    country,
    mozfun.bits28.retention(days_seen_bits, submission_date) AS retention_first_28_days,
    days_seen_bits,
  FROM
    firefox_ios_derived.clients_last_seen_joined_v1
  WHERE
    -- need to make submission_date date 1 day older due to 2 day lag inside the table
    submission_date = DATE_SUB(@submission_date, INTERVAL 1 DAY)
),
corresponding_first_seen_clients AS (
  SELECT
    client_id,
    sample_id,
    first_seen_date,
  FROM
    firefox_ios.baseline_clients_first_seen
  WHERE
    -- 29 instead of 28 to compensate for the 2 day lag inside clients_last_seen_joined_v1
    submission_date = DATE_SUB(@submission_date, INTERVAL 29 DAY)
)
SELECT
  client_id,
  sample_id,
  first_seen_date,
  country,
  retention_first_28_days,
  mozfun.bits28.active_in_range(days_seen_bits, -1, 1) AS active_on_day_1,
FROM
  first_28_day_activity
INNER JOIN
  corresponding_first_seen_clients
USING
  (client_id, sample_id)
