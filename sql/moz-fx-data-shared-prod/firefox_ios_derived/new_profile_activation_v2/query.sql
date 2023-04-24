-- This table implements the Mobile activation metric defined in June 2022
-- Based on data from the first seven days since profile creation, we select
-- clients with at least three days of use (first open plus two) and who
-- perform a search on the later half of the week. As such, this table will
-- always lag new profiles by seven days and the CTEs are filtered for
-- corresponding periods.
-- Each entry in this table corresponds to a new_profile
WITH dou AS (
  SELECT
    client_id,
    sample_id,
    first_seen_date,
    ARRAY_LENGTH(
      mozfun.bits28.to_dates(mozfun.bits28.range(days_seen_bits, -5, 6), submission_date)
    ) AS days_2_7,
  FROM
    firefox_ios.baseline_clients_last_seen
  WHERE
    submission_date = @submission_date
    AND DATE_DIFF(submission_date, first_seen_date, DAY) = 6
),
client_search AS (
  SELECT
    client_id,
    SUM(search_count) AS search_count
  FROM
    search_derived.mobile_search_clients_daily_v1
  WHERE
    (submission_date BETWEEN DATE_SUB(@submission_date, INTERVAL 3 DAY) AND @submission_date)
    AND os = 'iOS'
    AND normalized_app_name = 'Fennec'
  GROUP BY
    client_id
)
SELECT
  @submission_date AS `date`,
  first_seen_date,
  client_id,
  sample_id,
  TRUE AS is_new_profile,
  IF(days_2_7 > 1 AND COALESCE(search_count, 0) > 0, TRUE, FALSE) AS is_activated,
FROM
  dou
LEFT JOIN
  client_search
USING
  (client_id)
