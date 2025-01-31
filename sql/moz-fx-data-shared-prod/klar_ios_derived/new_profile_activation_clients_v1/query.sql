WITH new_profiles AS (
  SELECT
    first_seen_date,
    client_id,
    normalized_channel,
    app_name,
    app_version,
    country,
    city,
    geo_subdivision,
    locale,
    isp,
    os,
    os_version,
    device_model,
    device_manufacturer,
    is_mobile,
    device_type,
  FROM
    `moz-fx-data-shared-prod.klar_ios.new_profile_clients`
  WHERE
    first_seen_date = DATE_SUB(@submission_date, INTERVAL 6 DAY)
),
active_users AS (
  SELECT
    first_seen_date,
    client_id,
    ARRAY_LENGTH(
      mozfun.bits28.to_dates(mozfun.bits28.range(days_seen_bits, -5, 6), submission_date)
    ) AS num_days_seen_day_2_7,
    ARRAY_LENGTH(
      mozfun.bits28.to_dates(mozfun.bits28.range(days_active_bits, -5, 6), submission_date)
    ) AS num_days_active_day_2_7,
  FROM
    `moz-fx-data-shared-prod.klar_ios.active_users`
  WHERE
    submission_date = @submission_date
    AND DATE_DIFF(submission_date, first_seen_date, DAY) = 6
),
client_search AS (
  SELECT
    client_id,
    SUM(search_count) AS search_count,
  FROM
    `moz-fx-data-shared-prod.search.mobile_search_clients_daily`
  WHERE
    submission_date
    BETWEEN DATE_SUB(@submission_date, INTERVAL 3 DAY)
    AND @submission_date
    AND normalized_app_name_os = "Klar iOS"
  GROUP BY
    client_id
)
SELECT
  @submission_date AS submission_date,
  first_seen_date,
  client_id,
  normalized_channel,
  app_name,
  app_version,
  country,
  city,
  geo_subdivision,
  locale,
  isp,
  os,
  os_version,
  device_model,
  device_manufacturer,
  is_mobile,
  device_type,
  COALESCE(num_days_seen_day_2_7, 0) AS num_days_seen_day_2_7,
  COALESCE(num_days_active_day_2_7, 0) AS num_days_active_day_2_7,
  COALESCE(search_count, 0) AS search_count,
FROM
  new_profiles
INNER JOIN
  active_users
  USING (first_seen_date, client_id)
LEFT JOIN
  client_search
  USING (client_id)
