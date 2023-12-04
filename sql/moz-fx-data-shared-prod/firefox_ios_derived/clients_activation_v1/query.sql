-- This table implements the Mobile activation metric defined in June 2022
-- Based on data from the first seven days since profile creation, we select
-- clients with at least three days of use (first open plus two) and who
-- perform a search on the later half of the week. As such, this table will
-- always lag new profiles by seven days and the CTEs are filtered for
-- corresponding periods.
-- Each entry in this table corresponds to a new_profile
WITH new_clients AS (
  SELECT
    client_id,
    first_seen_date,
    sample_id,
    channel,
  FROM
    firefox_ios.firefox_ios_clients
  WHERE
    first_seen_date = DATE_SUB(@submission_date, INTERVAL 6 DAY)
),
new_clients_activity AS (
  SELECT
    client_id,
    first_seen_date,
    sample_id,
    normalized_channel AS channel,
    ARRAY_LENGTH(
      mozfun.bits28.to_dates(mozfun.bits28.range(days_seen_bits, -5, 6), submission_date)
    ) AS days_2_7,
  FROM
    firefox_ios.baseline_clients_last_seen
  WHERE
    submission_date = @submission_date
    AND DATE_DIFF(submission_date, first_seen_date, DAY) = 6
),
clients_search AS (
  SELECT
    client_id,
    sample_id,
    channel,
    SUM(search_count) AS search_count
  FROM
    search_derived.mobile_search_clients_daily_v1
  WHERE
    (submission_date BETWEEN DATE_SUB(@submission_date, INTERVAL 3 DAY) AND @submission_date)
    AND os = 'iOS'
    AND normalized_app_name = 'Fennec'
  GROUP BY
    client_id,
    sample_id,
    channel
)
SELECT
  @submission_date AS submission_date,
  first_seen_date,
  client_id,
  sample_id,
  IF(days_2_7 > 1 AND COALESCE(search_count, 0) > 0, TRUE, FALSE) AS is_activated,
FROM
  new_clients
LEFT JOIN
  new_clients_activity
USING
  (client_id, first_seen_date, sample_id, channel)
LEFT JOIN
  clients_search
USING
  (client_id, sample_id, channel)
