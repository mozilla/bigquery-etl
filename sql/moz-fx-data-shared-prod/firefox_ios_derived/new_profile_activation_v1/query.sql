-- This table implements the Mobile activation metric defined in June 2022
-- Based on data from the first seven days since profile creation, we select
-- clients with at least three days of use (first open plus two) and who
-- perform a search on the later half of the week. As such, this table will
-- always lag new profiles by seven days and the CTEs are filtered for
-- corresponding periods.
WITH client_first_seen AS (
  SELECT
    client_id,
    sample_id,
    normalized_channel,
    app_build,
    app_channel,
    app_display_version,
    device_manufacturer,
    device_model,
    isp,
    locale,
    city,
    country,
    first_seen_date
  FROM
    `moz-fx-data-shared-prod.firefox_ios.baseline_clients_first_seen`
  WHERE
    submission_date = DATE_SUB(@submission_date, INTERVAL 6 DAY)
),
client_search AS (
  SELECT
    client_id,
    SUM(search_count) AS search_count
  FROM
    `moz-fx-data-shared-prod.search_derived.mobile_search_clients_daily_v1`
  JOIN
    client_first_seen
    USING (client_id)
  WHERE
    (submission_date BETWEEN DATE_SUB(@submission_date, INTERVAL 3 DAY) AND @submission_date)
    AND normalized_app_name = 'Fennec'
    AND os = 'iOS'
  GROUP BY
    1
),
dou AS (
  SELECT
    client_id,
    submission_date,
    ARRAY_LENGTH(
      mozfun.bits28.to_dates(mozfun.bits28.range(days_seen_bits, -5, 6), submission_date)
    ) AS days_2_7,
  FROM
    `moz-fx-data-shared-prod.firefox_ios.baseline_clients_last_seen`
  WHERE
    submission_date = @submission_date
    AND DATE_DIFF(submission_date, first_seen_date, DAY) = 6
    AND normalized_channel = 'release'
)
SELECT
  client_id,
  sample_id,
  normalized_channel,
  app_build,
  app_channel,
  app_display_version,
  device_manufacturer,
  device_model,
  isp,
  locale,
  city,
  country,
  first_seen_date,
  submission_date,
  1 AS new_profile,
  CAST(days_2_7 > 1 AND COALESCE(search_count, 0) > 0 AS integer) AS activated
FROM
  dou
INNER JOIN
  client_first_seen
  USING (client_id)
LEFT JOIN
  client_search
  USING (client_id)
