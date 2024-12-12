-- Query for contextual_services_derived.sponsored_tiles_forecasting_v1
            -- For more information on writing queries see:
            -- https://docs.telemetry.mozilla.org/cookbooks/bigquery/querying.html
-- DAU SHARE BY COUNTRY
-- used for clicks forecasts
WITH client_counts AS (
  SELECT
    country,
  -- want qualified desktop clients, any mobile clients
    (
      CASE
      WHEN
        normalized_app_name = "Firefox Desktop"
        AND active_hours_sum > 0
        AND uri_count > 0
      THEN
        'desktop'
      WHEN
        normalized_app_name != "Firefox Desktop"
      THEN
        'mobile'
      ELSE
        NULL
      END
    ) AS device,
    submission_date,
    COUNT(*) AS total_clients,
    COUNT(
      CASE
  -- FIREFOX DESKTOP ELIGIBILITY REQUIREMENTS
      WHEN
        normalized_app_name = "Firefox Desktop"
        AND (
            -- desktop tiles default on
          (
            submission_date >= "2021-09-07"
            AND browser_version_info.major_version > 92
            AND country IN UNNEST(
              ["AU", "BR", "CA", "DE", "ES", "FR", "GB", "IN", "IT", "MX", "US"]
            )
          )
          OR
            -- Japan desktop now default on
          (
            submission_date >= "2022-01-25"
            AND browser_version_info.major_version > 92
            AND country = "JP"
          )
        )
      THEN
        1
-- ANDROID ELIGIBLITY REQUIREMENTS
      WHEN
        normalized_app_name != "Firefox Desktop"
        AND normalized_os = "Android"
        AND browser_version_info.major_version > 100
        AND (
          (country IN UNNEST(["US"]) AND submission_date >= "2022-05-10")
          OR (country IN UNNEST(["DE"]) AND submission_date >= "2022-12-05")
        )
      THEN
        1
  -- iOS ELIGIBLITY REQUIREMENTS
      WHEN
        normalized_app_name != "Firefox Desktop"
        AND normalized_os = "iOS"
        AND browser_version_info.major_version > 101
        AND (
          (country IN UNNEST(["US"]) AND submission_date >= "2022-06-07")
          OR (country IN UNNEST(["DE"]) AND submission_date >= "2022-12-05")
        )
      THEN
        1
      ELSE
        NULL
      END
    ) AS eligible_clients
  FROM
    mozdata.telemetry.unified_metrics
  WHERE
    mozfun.bits28.active_in_range(days_seen_bits, 0, 1)
    AND submission_date = "2023-04-01" # >= "2021-09-07"
    AND sample_id < 10
  GROUP BY
    country,
    device,
    submission_date
),
grand_total AS (
  SELECT
    device,
    submission_date,
    SUM(total_clients) AS daily_total
  FROM
    client_counts
  WHERE
    device IS NOT NULL
  GROUP BY
    device,
    submission_date
),
client_share AS (
  SELECT
    device,
    country,
    submission_date,
    eligible_clients / NULLIF(daily_total, 0) AS eligible_share_country
  FROM
    client_counts
  LEFT JOIN
    grand_total
  USING
    (submission_date, device)
  WHERE
    device IS NOT NULL
),
-- client counts and inventory data
-- used for impression forecasts
activity_stream AS (
  SELECT
    normalized_country_code AS country,
    date(submission_timestamp) AS submission_date,
    'desktop' AS device,
    COUNT(*) AS session_count,
    COUNT(DISTINCT client_id) AS clients_tiles_enabled
  FROM
    `mozdata.activity_stream.sessions`
  WHERE
    (metadata.geo.country) IN (
      'AU',
      'BR',
      'CA',
      'DE',
      'ES',
      'FR',
      'GB',
      'IN',
      'IT',
      'JP',
      'MX',
      'US'
    )
    AND ((page) IN ('about:home', 'about:newtab') AND (user_prefs) >= 258)
    AND `mozfun`.norm.browser_version_info(version).major_version >= 91
    AND date(submission_timestamp) = "2023-04-01"
    AND sample_id = 1
  GROUP BY
    1,
    2,
    3
  ORDER BY
    1,
    2
),
-------- REVENUE FORECASTING DATA
-- number of active clients enrolled in sponsored topsites each day (by country)
-- and their usage metrics
desktop_mobile_population AS (
  SELECT
    submission_date,
    country,
    device,
    COUNT(client_id) AS eligible_clients,
    COUNT(
      CASE
      WHEN
        sponsored_tiles_impression_count > 0
      THEN
        client_id
      ELSE
        NULL
      END
    ) AS exposed_clients,
    SUM(sponsored_tiles_click_count) AS clicks,
    SUM(sponsored_tiles_impression_count) AS impressions
  FROM
    `mozdata.telemetry.sponsored_tiles_clients_daily`
  WHERE
    submission_date = "2023-04-01" # >= "2021-09-07"
    AND sample_id = 1
  GROUP BY
    submission_date,
    country,
    device
)
-- number of clicks and client-days-of-use by advertiser (and country and month)
-- daily AS (
SELECT
  submission_date,
  country,
  device,
  -- client measures
  client_share.eligible_share_country,
  COALESCE(desktop_mobile_population.eligible_clients, 0) AS eligible_clients,
  COALESCE(activity_stream.clients_tiles_enabled, 0) AS clients_tiles_enabled,
  COALESCE(desktop_mobile_population.exposed_clients, 0) AS exposed_clients,
  COALESCE(desktop_mobile_population.exposed_clients, 0) / COALESCE(
    desktop_mobile_population.eligible_clients,
    0
  ) AS exposure_rate,
  COALESCE(activity_stream.session_count, 0) AS session_count,
  COALESCE(activity_stream.session_count * 2, 0) AS total_inventory,
  -- engagement events
  COALESCE(desktop_mobile_population.clicks, 0) AS clicks,
  COALESCE(desktop_mobile_population.impressions, 0) AS impressions,
  COALESCE(
    desktop_mobile_population.impressions / (activity_stream.session_count * 2),
    0
  ) AS impression_display_rate,
  -- usage per client-day-of-use
  COALESCE(desktop_mobile_population.clicks, 0) / COALESCE(
    desktop_mobile_population.exposed_clients,
    0
  ) AS clicks_per_exposed_client,
  COALESCE(desktop_mobile_population.impressions, 0) / COALESCE(
    desktop_mobile_population.exposed_clients,
    0
  ) AS impressions_per_exposed_client,
  -- COALESCE(activity_stream.session_count*2, 0) / COALESCE(desktop_mobile_population.exposed_clients, 0) AS inventory_per_exposed_client,
FROM
  desktop_mobile_population
LEFT JOIN
  client_share
USING
  (device, country, submission_date)
LEFT JOIN
  activity_stream
USING
  (device, country, submission_date)
-- WHERE
--   submission_date = @submission_date
ORDER BY
  submission_date,
  country,
  device
