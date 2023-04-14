-- Query for contextual_services_derived.suggest_foreasting_v1
            -- For more information on writing queries see:
            -- https://docs.telemetry.mozilla.org/cookbooks/bigquery/querying.html
-- DAU SHARE BY COUNTRY
-- Used to scale automated global KPI forecasts down to US clients only
WITH client_counts AS (
  SELECT
    country,
    submission_date,
    COUNT(*) AS total_clients,
    COUNT(
      CASE
  -- SUGGEST DESKTOP ELIGIBILITY REQUIREMENTS
        WHEN normalized_app_name = "Firefox Desktop"
          AND
            -- desktop Suggest experiment start -- 12.5% exposure until 2022-09-21
          (
            submission_date >= "2022-06-07"
            AND browser_version_info.major_version > 92
            AND country IN UNNEST(["US"])
          )
          THEN 1
        ELSE NULL
      END
    ) AS eligible_clients
  FROM
    telemetry.unified_metrics
  WHERE
    mozfun.bits28.active_in_range(days_seen_bits, 0, 1)
    AND submission_date >= "2022-06-07"
    AND sample_id < 10
    AND normalized_app_name = "Firefox Desktop"
    # desktop KPI = qualified clients
    AND active_hours_sum > 0
    AND uri_count > 0
  GROUP BY
    country,
    submission_date
),
grand_total AS (
  SELECT
    submission_date,
    SUM(total_clients) AS monthly_total
  FROM
    client_counts
  GROUP BY
    submission_date
),
client_share AS (
  SELECT
    country,
    submission_date,
    eligible_clients / NULLIF(monthly_total, 0) AS eligible_share_country
  FROM
    client_counts
  LEFT JOIN
    grand_total
  USING
    (submission_date)
),
-------- REVENUE FORECASTING DATA
-- Suggest desktop population & usage metrics
desktop_population AS (
  SELECT
    submission_date,
    country,
    "desktop" AS device,
    COUNT(client_id) AS suggest_eligible_clients,
    COUNT(
      CASE
        WHEN impression_sponsored_count > 0
          THEN client_id
        ELSE NULL
      END
    ) AS suggest_exposed_clients,
    SUM(impression_sponsored_count) AS impressions,
    SUM(click_sponsored_count) AS clicks
  FROM
    telemetry.suggest_clients_daily
  WHERE
    submission_date >= "2022-06-07"
    AND normalized_channel = "release"
    AND (
      browser_version_info.major_version >= 92
      OR (
        mozfun.map.get_key(experiments, 'firefox-suggest-enhanced-exposure-phase-1') = "treatment-a"
      )
    )
    AND country IN UNNEST(["US"])
  GROUP BY
    submission_date,
    country,
    device
)
-- number of clicks and client-days-of-use (by country and month)
-- daily AS (
SELECT
  submission_date,
  desktop_population.country,
  device,
  client_share.eligible_share_country,
  COALESCE(desktop_population.suggest_eligible_clients, 0) AS suggest_eligible_clients,
  COALESCE(desktop_population.suggest_exposed_clients, 0) AS suggest_exposed_clients,
  COALESCE(desktop_population.suggest_exposed_clients, 0) / COALESCE(
    desktop_population.suggest_eligible_clients,
    0
  ) AS exposure_rate,
  desktop_population.clicks,
  desktop_population.impressions,
    -- usage per client-day-of-use
  (
    desktop_population.clicks / NULLIF((desktop_population.suggest_exposed_clients), 0)
  ) AS clicks_per_exposed_client,
  (
    desktop_population.impressions / NULLIF((desktop_population.suggest_exposed_clients), 0)
  ) AS impressions_per_exposed_client
FROM
  desktop_population
LEFT JOIN
  client_share
USING
  (country, submission_date)
WHERE
  submission_date = @submission_date
ORDER BY
  submission_date,
  country,
  device
