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
),
-- we need to check if client_id already exists in the table
-- to address an edge case where client_id's first_seen_date.
--
-- !!! This can result in variance between the first_seen_date
-- value reported by firefox_ios.baseline_first_seen
-- and this table !!!
previous_client_entries AS (
  SELECT
    client_id
  FROM
    firefox_ios_derived.new_profile_activation_v2
  WHERE
    `date` < @submission_date
)
SELECT
  @submission_date AS `date`,
  dou.first_seen_date,
  dou.client_id,
  dou.sample_id,
  TRUE AS is_new_profile,
  IF(dou.days_2_7 > 1 AND COALESCE(client_search.search_count, 0) > 0, TRUE, FALSE) AS is_activated,
FROM
  dou
LEFT JOIN
  client_search
  USING (client_id)
LEFT JOIN
  previous_client_entries
  USING (client_id)
WHERE
  previous_client_entries.client_id IS NULL
