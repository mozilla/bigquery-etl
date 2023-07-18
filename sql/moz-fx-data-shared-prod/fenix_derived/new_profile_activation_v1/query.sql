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
    `moz-fx-data-shared-prod`.fenix.baseline_clients_first_seen
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
  USING
    (client_id)
  WHERE
    (submission_date BETWEEN DATE_SUB(@submission_date, INTERVAL 3 DAY) AND @submission_date)
    AND normalized_app_name = 'Fenix'
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
    `moz-fx-data-shared-prod.fenix.baseline_clients_last_seen`
  WHERE
    submission_date = @submission_date
    AND DATE_DIFF(submission_date, first_seen_date, DAY) = 6
),
adjust_client AS (
  SELECT
    client_info.client_id AS client_id,
    ARRAY_AGG(metrics.string.first_session_network)[SAFE_OFFSET(0)] AS adjust_network,
    ARRAY_AGG(metrics.string.first_session_adgroup)[SAFE_OFFSET(0)] AS adjust_adgroup,
    ARRAY_AGG(metrics.string.first_session_campaign)[SAFE_OFFSET(0)] AS adjust_campaign,
    ARRAY_AGG(metrics.string.first_session_creative)[SAFE_OFFSET(0)] AS adjust_creative,
    MIN(DATE(submission_timestamp)) AS first_session_date
  FROM
    `moz-fx-data-shared-prod.fenix.first_session`
  WHERE
    (
      DATE(submission_timestamp)
      BETWEEN DATE_SUB(@submission_date, INTERVAL 6 DAY)
      AND @submission_date
    )
    AND metrics.string.first_session_network IS NOT NULL
    AND metrics.string.first_session_network <> ''
  GROUP BY
    1
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
  COALESCE(adjust_network, 'Unknown') AS adjust_network,
  COALESCE(adjust_adgroup, 'Unknown') AS adjust_adgroup,
  COALESCE(adjust_campaign, 'Unknown') AS adjust_campaign,
  COALESCE(adjust_creative, 'Unknown') AS adjust_creative,
  first_seen_date,
  submission_date,
  1 AS new_profile,
  CAST(days_2_7 > 1 AND COALESCE(search_count, 0) > 0 AS integer) AS activated
FROM
  dou
INNER JOIN
  client_first_seen
USING
  (client_id)
LEFT JOIN
  client_search
USING
  (client_id)
LEFT JOIN
  adjust_client
USING
  (client_id)
