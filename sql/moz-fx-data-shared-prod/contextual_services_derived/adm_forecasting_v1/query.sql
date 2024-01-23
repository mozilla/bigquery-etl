-- Query for contextual_services_derived.adm_forecasting_v1
            -- For more information on writing queries see:
            -- https://docs.telemetry.mozilla.org/cookbooks/bigquery/querying.html
-- DAU SHARE BY COUNTRY
WITH client_counts AS (
  SELECT
    country,
  -- want qualified desktop clients, any mobile clients
    (
      CASE
        WHEN normalized_app_name = "Firefox Desktop"
          AND active_hours_sum > 0
          AND uri_count > 0
          THEN 'desktop'
        WHEN normalized_app_name != "Firefox Desktop"
          THEN 'mobile'
        ELSE NULL
      END
    ) AS device,
    submission_date,
    COUNT(*) AS total_clients,
    COUNT(
      CASE
  -- FIREFOX DESKTOP ELIGIBILITY REQUIREMENTS
        WHEN normalized_app_name = "Firefox Desktop"
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
          THEN 1
-- ANDROID ELIGIBLITY REQUIREMENTS
        WHEN normalized_app_name != "Firefox Desktop"
          AND normalized_os = "Android"
          AND browser_version_info.major_version > 100
          AND (
            (country IN UNNEST(["US"]) AND submission_date >= "2022-05-10")
            OR (country IN UNNEST(["DE"]) AND submission_date >= "2022-12-05")
          )
          THEN 1
  -- iOS ELIGIBLITY REQUIREMENTS
        WHEN normalized_app_name != "Firefox Desktop"
          AND normalized_os = "iOS"
          AND browser_version_info.major_version > 101
          AND (
            (country IN UNNEST(["US"]) AND submission_date >= "2022-06-07")
            OR (country IN UNNEST(["DE"]) AND submission_date >= "2022-12-05")
          )
          THEN 1
        ELSE NULL
      END
    ) AS eligible_clients
  FROM
    telemetry.unified_metrics
  WHERE
    mozfun.bits28.active_in_range(days_seen_bits, 0, 1)
    AND submission_date >= "2021-09-07"
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
    SUM(total_clients) AS monthly_total
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
    eligible_clients / NULLIF(monthly_total, 0) AS eligible_share_country
  FROM
    client_counts
  LEFT JOIN
    grand_total
    USING (submission_date, device)
  WHERE
    device IS NOT NULL
),
-------- REVENUE FORECASTING DATA
tiles_percentages AS (
  SELECT
    "sponsored_tiles" AS product,
    submission_date,
    country,
    CASE
      WHEN form_factor = "phone"
        THEN "mobile"
      ELSE "desktop"
    END AS device,
    SUM(CASE WHEN advertiser = "amazon" THEN user_count ELSE 0 END) / NULLIF(
      SUM(user_count),
      0
    ) AS p_amazon,
    SUM(
      CASE
        WHEN advertiser NOT IN UNNEST(["amazon", "o=45:a", "yandex"])
          THEN user_count
        ELSE 0
      END
    ) / NULLIF(SUM(user_count), 0) AS p_other
  FROM
    contextual_services.event_aggregates
  WHERE
    submission_date >= "2021-09-07"
    AND release_channel = "release"
    AND event_type = "impression"
    AND source = "topsites"
    AND country IN UNNEST(["AU", "BR", "CA", "DE", "ES", "FR", "GB", "IN", "IT", "JP", "MX", "US"])
  GROUP BY
    product,
    submission_date,
    country,
    device
),
suggest_percentages AS (
  SELECT
    "suggest" AS product,
    submission_date,
    country,
    CASE
      WHEN form_factor = "phone"
        THEN "mobile"
      ELSE "desktop"
    END AS device,
    NULL AS p_amazon,
    NULL AS p_other,
    SUM(CASE WHEN advertiser = "amazon" THEN user_count ELSE 0 END) AS amazon_dou,
    SUM(
      CASE
        WHEN advertiser NOT IN UNNEST(["amazon", "wikipedia"])
          THEN user_count
        ELSE 0
      END
    ) AS other_dou,
  FROM
    contextual_services.event_aggregates
  WHERE
    submission_date >= "2022-06-07"
    AND release_channel = "release"
    AND event_type = "impression"
    AND source = "suggest"
    AND country IN UNNEST(["US"])
  GROUP BY
    product,
    submission_date,
    country,
    device
),
-- number of active clients enrolled in sponsored topsites each day (by country)
desktop_population AS (
-- Sponsored Tiles desktop eligible population
  SELECT
    "sponsored_tiles" AS product,
    submission_date,
    country,
    "desktop" AS device,
    COUNT(DISTINCT client_id) AS clients
  FROM
    telemetry.clients_daily
  CROSS JOIN
    UNNEST(experiments) AS experiment
  WHERE
    submission_date >= "2021-09-07"
    AND normalized_channel = "release"
    AND active_hours_sum > 0
    AND scalar_parent_browser_engagement_total_uri_count_sum > 0
    AND (
      (
            --sponsored tiles experiments
        experiment.key IN (
                -- experiment end date Aug 10, 2021
          "bug-1678683-pref-topsites-launch-phase-3-us-v2-release-83-85",
          "bug-1676315-pref-topsites-launch-phase-3-gb-release-83-85",
          "bug-1676316-pref-topsites-launch-phase-3-de-release-83-85",
          "bug-1682645-pref-topsites-launch-phase3-group2-au-release-84-86",
          "bug-1682644-pref-topsites-launch-phase3-group2-ca-release-84-86",
          "bug-1682646-pref-topsites-launch-phase3-group2-fr-release-84-86",
                -- experiment end date Dec 14, 2021
          "bug-1693420-rollout-sponsored-top-sites-rollout-release-84-100",
                -- experiment end date Jan 25 2022
          'bug-1744371-pref-sponsored-tiles-in-japan-experiment-release-92-95'
        )
        AND experiment.value IN ("treatment-admarketplace", "active")
      )
      OR (
            --sponsored tiles on by default in Fx 92
        country IN UNNEST(["AU", "BR", "CA", "DE", "ES", "FR", "GB", "IN", "IT", "MX", "US"])
        AND mozfun.norm.truncate_version(app_version, 'major') >= 92
        AND submission_date >= "2021-09-07"
      )
      OR (
            -- Japan Tiles not fully released until January 2022 after December rollout
        country IN ("JP")
        AND mozfun.norm.truncate_version(app_version, 'major') >= 92
        AND submission_date >= "2022-01-25"
      )
    )
    AND country IN UNNEST(["AU", "BR", "CA", "DE", "ES", "FR", "GB", "IN", "IT", "JP", "MX", "US"])
    -- AND sample_id = 1
  GROUP BY
    product,
    submission_date,
    country,
    device
  UNION ALL
-- Suggest desktop eligible population
  SELECT
    "suggest" AS product,
    submission_date,
    country,
    "desktop" AS device,
    COUNT(DISTINCT client_id) AS clients
  FROM
    telemetry.clients_daily
  CROSS JOIN
    UNNEST(experiments) AS experiment
  WHERE
    submission_date >= "2022-06-07"
    AND normalized_channel = "release"
    AND active_hours_sum > 0
    AND scalar_parent_browser_engagement_total_uri_count_sum > 0
    AND (
            --sponsored tiles experiments
      experiment.key IN (
                -- experiment end date expected August - September 2022
        "firefox-suggest-enhanced-exposure-phase-1"
      )
      AND experiment.value IN ("treatment-a")
    )
    AND country IN UNNEST(["US"])
    -- AND sample_id = 1
  GROUP BY
    product,
    submission_date,
    country,
    device
),
mobile_experiment_clients AS (
  SELECT
    client_id
  FROM
    `moz-fx-data-experiments.mozanalysis.enrollments_firefox_android_sponsored_shortcuts_experiment`
  WHERE
    branch = "treatment-a"
  UNION ALL
  SELECT
    client_id
  FROM
    `moz-fx-data-experiments.mozanalysis.enrollments_firefox_ios_homepage_experiment_sponsored_shortcuts`
  WHERE
    branch = "treatment-a"
),
-- mobile = Sponsored Tiles only
-- total mobile clients per day from each OS
daily_mobile_clients AS (
  -- experiment clients
  SELECT
    *
  FROM
    mobile_experiment_clients
  LEFT JOIN
    (
      SELECT
        submission_date,
        client_id,
        country
      FROM
        telemetry.unified_metrics AS browser_dau
      WHERE
        mozfun.bits28.active_in_range(browser_dau.days_seen_bits, 0, 1)
            -- don't want Focus apps
        AND browser_dau.normalized_app_name IN ('Fenix', "Firefox iOS")
        AND country IN UNNEST(["US"])
        AND normalized_channel = "release"
        -- AND sample_id = 1
        AND (submission_date BETWEEN "2022-05-10" AND "2022-10-03")
        AND (
          (normalized_app_name = "Fenix" AND submission_date BETWEEN "2022-05-10" AND "2022-09-19")
          OR (
            normalized_app_name = "Firefox iOS"
            AND (submission_date BETWEEN "2022-06-07" AND "2022-10-03")
          )
        )
    )
    USING (client_id)
  WHERE
    submission_date >= "2022-05-10"
  -- then mobile tiles went to default
  UNION ALL
  SELECT
    client_id,
    submission_date,
    country
  FROM
    telemetry.unified_metrics AS browser_dau
  WHERE
    mozfun.bits28.active_in_range(browser_dau.days_seen_bits, 0, 1)
    -- don't want Focus apps
    AND browser_dau.normalized_app_name IN ('Fenix', "Firefox iOS")
    AND normalized_channel = "release"
    AND submission_date >= "2022-09-20"
    AND (
      (
        normalized_app_name = "Fenix"
        AND (
          (submission_date >= "2022-09-20" AND country IN UNNEST(["US"]))
          OR (submission_date >= "2022-12-05" AND country IN UNNEST(["DE"]))
        )
      )
      OR (
        normalized_app_name = "Firefox iOS"
        AND (
          (submission_date >= "2022-10-04" AND country IN UNNEST(["US"]))
          OR (submission_date >= "2022-12-05" AND country IN UNNEST(["DE"]))
        )
      )
    )
    -- AND sample_id = 1
),
-- total mobile clients per day
mobile_population AS (
  SELECT
    "sponsored_tiles" AS product,
    submission_date,
    country,
    "mobile" AS device,
    COUNT(*) AS clients
  FROM
    daily_mobile_clients
  GROUP BY
    product,
    submission_date,
    country,
    device
),
-- total desktop and mobile clients per day
population AS (
  SELECT
    product,
    submission_date,
    country,
    device,
    clients
  FROM
    desktop_population
  UNION ALL
  SELECT
    product,
    submission_date,
    country,
    device,
    clients
  FROM
    mobile_population
),
-- number of clicks by advertiser (and country and user-selected-time-interval)
clicks AS (
  SELECT
    "sponsored_tiles" AS product,
    submission_date,
    country,
    CASE
      WHEN form_factor = "phone"
        THEN "mobile"
      ELSE "desktop"
    END AS device,
    COALESCE(SUM(CASE WHEN advertiser = "amazon" THEN event_count ELSE 0 END), 0) AS amazon_clicks,
    COALESCE(
      SUM(
        CASE
          WHEN advertiser NOT IN UNNEST(["amazon", "o=45:a", "yandex"])
            THEN event_count
          ELSE 0
        END
      ),
      0
    ) AS other_clicks
  FROM
    contextual_services.event_aggregates
  WHERE
    submission_date >= "2021-09-07"
    AND release_channel = "release"
    AND event_type = "click"
    AND source = "topsites"
    AND country IN UNNEST(["AU", "BR", "CA", "DE", "ES", "FR", "GB", "IN", "IT", "JP", "MX", "US"])
  GROUP BY
    product,
    submission_date,
    country,
    device
  UNION ALL
  SELECT
    "suggest" AS product,
    submission_date,
    country,
    CASE
      WHEN form_factor = "phone"
        THEN "mobile"
      ELSE "desktop"
    END AS device,
    COALESCE(SUM(CASE WHEN advertiser = "amazon" THEN event_count ELSE 0 END), 0) AS amazon_clicks,
    COALESCE(
      SUM(CASE WHEN advertiser NOT IN UNNEST(["amazon", "wikipedia"]) THEN event_count ELSE 0 END),
      0
    ) AS other_clicks
  FROM
    contextual_services.event_aggregates
  WHERE
    submission_date >= "2022-06-07"
    AND release_channel = "release"
    AND event_type = "click"
    AND source = "suggest"
    AND country IN UNNEST(["US"])
  GROUP BY
    product,
    submission_date,
    country,
    device
)
-- number of clicks and client-days-of-use by advertiser (and country and month)
-- daily AS (
SELECT
  product,
  submission_date,
  population.country,
  device,
  client_share.eligible_share_country,
    -- Tiles clients are not directly tagged with advertiser, this must be imputed using impression share
    -- Limitation: This undercounts due to dual-Tile display model.
  COALESCE(population.clients, 0) AS clients,
  (CASE WHEN product = "sponsored_tiles" THEN pe.p_amazon ELSE NULL END) AS p_amazon,
  (CASE WHEN product = "sponsored_tiles" THEN pe.p_other ELSE NULL END) AS p_other,
  (
    CASE
      WHEN product = "sponsored_tiles"
        THEN COALESCE(population.clients * pe.p_amazon, 0)
      ELSE suggest_percentages.amazon_dou
    END
  ) AS amazon_clients,
  (
    CASE
      WHEN product = "sponsored_tiles"
        THEN COALESCE(population.clients * pe.p_other, 0)
      ELSE suggest_percentages.other_dou
    END
  ) AS other_clients,
    -- clicks are directly tagged with advertiser
  COALESCE(c.amazon_clicks, 0) AS amazon_clicks,
  COALESCE(c.other_clicks, 0) AS other_clicks,
    -- clicks per client-day-of-use
  (
    CASE
      WHEN product = "sponsored_tiles"
        THEN c.amazon_clicks / NULLIF((population.clients * pe.p_amazon), 0)
      ELSE c.amazon_clicks / NULLIF(suggest_percentages.amazon_dou, 0)
    END
  ) AS amazon_clicks_per_client,
  (
    CASE
      WHEN product = "sponsored_tiles"
        THEN c.other_clicks / NULLIF((population.clients * pe.p_other), 0)
      ELSE c.other_clicks / NULLIF(suggest_percentages.other_dou, 0)
    END
  ) AS other_clicks_per_client
FROM
  population
LEFT JOIN
  tiles_percentages pe
  USING (product, submission_date, country, device)
LEFT JOIN
  suggest_percentages
  USING (product, submission_date, country, device)
LEFT JOIN
  clicks c
  USING (product, submission_date, country, device)
LEFT JOIN
  client_share
  USING (device, country, submission_date)
WHERE
  submission_date = @submission_date
ORDER BY
  product,
  submission_date,
  country,
  device
