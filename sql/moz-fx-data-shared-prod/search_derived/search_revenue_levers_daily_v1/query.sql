WITH
-- Google Desktop (search + DAU)
desktop_data_google AS (
  SELECT
    submission_date,
    IF(LOWER(channel) LIKE '%esr%', 'esr', 'personal') AS channel,
    IF(country = 'US', 'US', 'RoW') AS country,
    COUNT(DISTINCT client_id) AS dau,
    COUNT(
      DISTINCT IF(default_search_engine LIKE '%google%', client_id, NULL)
    ) AS dau_w_engine_as_default,
    COUNT(
      DISTINCT IF(
        sap > 0
        AND normalized_engine = 'Google'
        AND default_search_engine LIKE '%google%',
        client_id,
        NULL
      )
    ) AS dau_engaged_w_sap,
    SUM(IF(normalized_engine = 'Google', sap, 0)) AS sap,
    SUM(IF(normalized_engine = 'Google', tagged_sap, 0)) AS tagged_sap,
    SUM(IF(normalized_engine = 'Google', tagged_follow_on, 0)) AS tagged_follow_on,
    SUM(IF(normalized_engine = 'Google', search_with_ads, 0)) AS search_with_ads,
    SUM(IF(normalized_engine = 'Google', ad_click, 0)) AS ad_click,
    SUM(IF(normalized_engine = 'Google', organic, 0)) AS organic,
    SUM(IF(normalized_engine = 'Google', ad_click_organic, 0)) AS ad_click_organic,
    SUM(IF(normalized_engine = 'Google', search_with_ads_organic, 0)) AS search_with_ads_organic,
    SUM(IF(normalized_engine = 'Google' AND is_sap_monetizable, sap, 0)) AS monetizable_sap
  FROM
    `moz-fx-data-shared-prod.search.search_clients_engines_sources_daily`
  WHERE
    submission_date = @submission_date
    AND country NOT IN ('RU', 'UA', 'TR', 'BY', 'KZ', 'CN')
  GROUP BY
    submission_date,
    channel,
    country
  ORDER BY
    submission_date,
    channel,
    country
),
-- Bing Desktop (non-Acer)
desktop_data_bing AS (
  SELECT
    submission_date,
    IF(country = 'US', 'US', 'RoW') AS country,
    COUNT(DISTINCT client_id) AS dau,
    COUNT(
      DISTINCT IF(default_search_engine LIKE '%bing%', client_id, NULL)
    ) AS dau_w_engine_as_default,
    COUNT(
      DISTINCT IF(
        sap > 0
        AND normalized_engine = 'Bing'
        AND default_search_engine LIKE '%bing%',
        client_id,
        NULL
      )
    ) AS dau_engaged_w_sap,
    SUM(IF(normalized_engine = 'Bing', sap, 0)) AS sap,
    SUM(IF(normalized_engine = 'Bing', tagged_sap, 0)) AS tagged_sap,
    SUM(IF(normalized_engine = 'Bing', tagged_follow_on, 0)) AS tagged_follow_on,
    SUM(IF(normalized_engine = 'Bing', search_with_ads, 0)) AS search_with_ads,
    SUM(IF(normalized_engine = 'Bing', ad_click, 0)) AS ad_click,
    SUM(IF(normalized_engine = 'Bing', organic, 0)) AS organic,
    SUM(IF(normalized_engine = 'Bing', ad_click_organic, 0)) AS ad_click_organic,
    SUM(IF(normalized_engine = 'Bing', search_with_ads_organic, 0)) AS search_with_ads_organic,
    SUM(IF(normalized_engine = 'Bing' AND is_sap_monetizable, sap, 0)) AS monetizable_sap
  FROM
    `moz-fx-data-shared-prod.search.search_clients_engines_sources_daily`
  WHERE
    submission_date = @submission_date
    AND (distribution_id IS NULL OR distribution_id NOT LIKE '%acer%')
    AND client_id NOT IN (SELECT client_id FROM `moz-fx-data-shared-prod.search.acer_cohort`)
  GROUP BY
    submission_date,
    country
  ORDER BY
    submission_date,
    country
),
-- DDG Desktop + Extension
desktop_data_ddg AS (
  SELECT
    submission_date,
    IF(country = 'US', 'US', 'RoW') AS country,
    COUNT(DISTINCT client_id) AS dau,
    COUNT(
      DISTINCT IF(
        (
          default_search_engine LIKE('%ddg%')
          OR default_search_engine LIKE('%duckduckgo%')
          AND NOT default_search_engine LIKE('%addon%')
        ),
        client_id,
        NULL
      )
    ) AS ddg_dau_w_engine_as_default,
    COUNT(
      DISTINCT IF(
        (engine) IN ('ddg', 'duckduckgo','ddg-lm','duckduckgo-lm')
        AND sap > 0
        AND (
          default_search_engine LIKE('%ddg%')
          OR default_search_engine LIKE('%duckduckgo%')
          AND NOT default_search_engine LIKE('%addon%')
        ),
        client_id,
        NULL
      )
    ) AS ddg_dau_engaged_w_sap,
    SUM(IF(engine IN ('ddg', 'duckduckgo','ddg-lm','duckduckgo-lm'), sap, 0)) AS ddg_sap,
    SUM(IF(engine IN ('ddg', 'duckduckgo','ddg-lm','duckduckgo-lm'), tagged_sap, 0)) AS ddg_tagged_sap,
    SUM(IF(engine IN ('ddg', 'duckduckgo','ddg-lm','duckduckgo-lm'), tagged_follow_on, 0)) AS ddg_tagged_follow_on,
    SUM(IF(engine IN ('ddg', 'duckduckgo','ddg-lm','duckduckgo-lm'), search_with_ads, 0)) AS ddg_search_with_ads,
    SUM(IF(engine IN ('ddg', 'duckduckgo','ddg-lm','duckduckgo-lm'), ad_click, 0)) AS ddg_ad_click,
    SUM(IF(engine IN ('ddg', 'duckduckgo','ddg-lm','duckduckgo-lm'), organic, 0)) AS ddg_organic,
    SUM(IF(engine IN ('ddg', 'duckduckgo','ddg-lm','duckduckgo-lm'), ad_click_organic, 0)) AS ddg_ad_click_organic,
    SUM(
      IF(engine IN ('ddg', 'duckduckgo','ddg-lm','duckduckgo-lm'), search_with_ads_organic, 0)
    ) AS ddg_search_with_ads_organic,
    SUM(IF(engine IN ('ddg', 'duckduckgo','ddg-lm','duckduckgo-lm') AND is_sap_monetizable, sap, 0)) AS ddg_monetizable_sap,
    -- in-content probes not available for addon so these metrics although being here will be zero
    COUNT(
      DISTINCT IF(default_search_engine LIKE('ddg%addon'), client_id, NULL)
    ) AS ddgaddon_dau_w_engine_as_default,
    COUNT(
      DISTINCT IF(
        engine = 'ddg-addon'
        AND sap > 0
        AND default_search_engine LIKE('ddg%addon'),
        client_id,
        NULL
      )
    ) AS ddgaddon_dau_engaged_w_sap,
    SUM(IF(engine IN ('ddg-addon'), sap, 0)) AS ddgaddon_sap,
    SUM(IF(engine IN ('ddg-addon'), tagged_sap, 0)) AS ddgaddon_tagged_sap,
    SUM(IF(engine IN ('ddg-addon'), tagged_follow_on, 0)) AS ddgaddon_tagged_follow_on,
    SUM(IF(engine IN ('ddg-addon'), search_with_ads, 0)) AS ddgaddon_search_with_ads,
    SUM(IF(engine IN ('ddg-addon'), ad_click, 0)) AS ddgaddon_ad_click,
    SUM(IF(engine IN ('ddg-addon'), organic, 0)) AS ddgaddon_organic,
    SUM(IF(engine IN ('ddg-addon'), ad_click_organic, 0)) AS ddgaddon_ad_click_organic,
    SUM(
      IF(engine IN ('ddg-addon'), search_with_ads_organic, 0)
    ) AS ddgaddon_search_with_ads_organic,
    SUM(IF(engine IN ('ddg-addon') AND is_sap_monetizable, sap, 0)) AS ddgaddon_monetizable_sap
  FROM
    `moz-fx-data-shared-prod.search.search_clients_engines_sources_daily`
  WHERE
    submission_date = @submission_date
  GROUP BY
    submission_date,
    country
  ORDER BY
    submission_date,
    country
),
-- Grab Mobile Eligible DAU
mobile_dau_data AS (
  SELECT
    submission_date,
    SUM(
      IF(country NOT IN ('US', 'RU', 'UA', 'BY', 'TR', 'KZ', 'CN'), dau, 0)
    ) AS row_dau_eligible_google,
    SUM(IF(country = 'US', dau, 0)) AS us_dau,
    SUM(IF(country != 'US', dau, 0)) AS row_dau,
    SUM(dau) AS dau
  FROM
    `moz-fx-data-shared-prod.telemetry.active_users_aggregates_device`
  WHERE
    submission_date = @submission_date
    AND app_name IN ('Fenix', 'Firefox iOS', 'Focus Android', 'Focus iOS')
  GROUP BY
    submission_date
),
-- Google Mobile (search only - as mobile search metrics is based on metrics
-- ping, while DAU should be based on main ping on Mobile, see also
-- https://mozilla-hub.atlassian.net/browse/RS-575)
mobile_data_google AS (
  SELECT
    submission_date,
    IF(country = 'US', 'US', 'RoW') AS country,
    IF(country = 'US', dau.us_dau, dau.row_dau_eligible_google) AS dau,
    COUNT(
      DISTINCT IF(default_search_engine LIKE '%google%', client_id, NULL)
    ) AS dau_w_engine_as_default,
    COUNT(
      DISTINCT IF(
        sap > 0
        AND normalized_engine = 'Google'
        AND default_search_engine LIKE '%google%',
        client_id,
        NULL
      )
    ) AS dau_engaged_w_sap,
    SUM(IF(normalized_engine = 'Google', sap, 0)) AS sap,
    SUM(IF(normalized_engine = 'Google', tagged_sap, 0)) AS tagged_sap,
    SUM(IF(normalized_engine = 'Google', tagged_follow_on, 0)) AS tagged_follow_on,
    SUM(IF(normalized_engine = 'Google', search_with_ads, 0)) AS search_with_ads,
    SUM(IF(normalized_engine = 'Google', ad_click, 0)) AS ad_click,
    SUM(IF(normalized_engine = 'Google', organic, 0)) AS organic,
    SUM(IF(normalized_engine = 'Google', ad_click_organic, 0)) AS ad_click_organic,
    -- metrics do not exist for mobile
    0 AS search_with_ads_organic,
    0 AS monetizable_sap
  FROM
    `moz-fx-data-shared-prod.search.mobile_search_clients_engines_sources_daily`
  INNER JOIN
    mobile_dau_data dau
  USING
    (submission_date)
  WHERE
    submission_date = @submission_date
    AND country NOT IN ('RU', 'UA', 'BY', 'TR', 'KZ', 'CN')
    AND (
      app_name IN ('Fenix', 'Firefox Preview', 'Focus', 'Focus Android Glean', 'Focus iOS Glean')
      OR (app_name = 'Fennec' AND os = 'iOS')
    )
  GROUP BY
    submission_date,
    country,
    dau
  ORDER BY
    submission_date,
    country,
    dau
),
-- Bing & DDG Mobile (search only - as mobile search metrics is based on
-- metrics ping, while DAU should be based on main ping on Mobile, see also
-- https://mozilla-hub.atlassian.net/browse/RS-575)
mobile_data_bing_ddg AS (
  SELECT
    submission_date,
    IF(country = 'US', 'US', 'RoW') AS country,
    IF(country = 'US', dau.us_dau, dau.row_dau) AS dau,
    COUNT(
      DISTINCT IF(default_search_engine LIKE '%bing%', client_id, NULL)
    ) AS bing_dau_w_engine_as_default,
    COUNT(
      DISTINCT IF(
        sap > 0
        AND normalized_engine = 'Bing'
        AND default_search_engine LIKE '%bing%',
        client_id,
        NULL
      )
    ) AS bing_dau_engaged_w_sap,
    COUNT(
      DISTINCT IF(
        default_search_engine LIKE('%ddg%')
        OR default_search_engine LIKE('%duckduckgo%'),
        client_id,
        NULL
      )
    ) AS ddg_dau_w_engine_as_default,
    COUNT(
      DISTINCT IF(
        (engine) IN ('ddg', 'duckduckgo')
        AND sap > 0
        AND (default_search_engine LIKE('%ddg%') OR default_search_engine LIKE('%duckduckgo%')),
        client_id,
        NULL
      )
    ) AS ddg_dau_engaged_w_sap,
    SUM(IF(normalized_engine = 'Bing', sap, 0)) AS bing_sap,
    SUM(IF(normalized_engine = 'Bing', tagged_sap, 0)) AS bing_tagged_sap,
    SUM(IF(normalized_engine = 'Bing', tagged_follow_on, 0)) AS bing_tagged_follow_on,
    SUM(IF(normalized_engine = 'Bing', search_with_ads, 0)) AS bing_search_with_ads,
    SUM(IF(normalized_engine = 'Bing', ad_click, 0)) AS bing_ad_click,
    SUM(IF(normalized_engine = 'Bing', organic, 0)) AS bing_organic,
    SUM(IF(normalized_engine = 'Bing', ad_click_organic, 0)) AS bing_ad_click_organic,
    -- metrics do not exist for mobile
    0 AS bing_search_with_ads_organic,
    0 AS bing_monetizable_sap,
    SUM(IF(normalized_engine = 'DuckDuckGo', sap, 0)) AS ddg_sap,
    SUM(IF(normalized_engine = 'DuckDuckGo', tagged_sap, 0)) AS ddg_tagged_sap,
    SUM(IF(normalized_engine = 'DuckDuckGo', tagged_follow_on, 0)) AS ddg_tagged_follow_on,
    SUM(IF(normalized_engine = 'DuckDuckGo', search_with_ads, 0)) AS ddg_search_with_ads,
    SUM(IF(normalized_engine = 'DuckDuckGo', ad_click, 0)) AS ddg_ad_click,
    SUM(IF(normalized_engine = 'DuckDuckGo', organic, 0)) AS ddg_organic,
    SUM(IF(normalized_engine = 'DuckDuckGo', ad_click_organic, 0)) AS ddg_ad_click_organic,
    -- metrics do not exist for mobile
    0 AS ddg_search_with_ads_organic,
    0 AS ddg_monetizable_sap
  FROM
    `moz-fx-data-shared-prod.search.mobile_search_clients_engines_sources_daily`
  INNER JOIN
    mobile_dau_data dau
  USING
    (submission_date)
  WHERE
    submission_date = @submission_date
    AND (
      app_name IN ('Fenix', 'Firefox Preview', 'Focus', 'Focus Android Glean', 'Focus iOS Glean')
      OR (app_name = 'Fennec' AND os = 'iOS')
    )
  GROUP BY
    submission_date,
    country,
    dau
  ORDER BY
    submission_date,
    country,
    dau
)
-- combine all desktop and mobile together
SELECT
  submission_date,
  'Google' AS partner,
  'desktop' AS device,
  channel,
  country,
  dau,
  dau_engaged_w_sap,
  sap,
  tagged_sap,
  tagged_follow_on,
  search_with_ads,
  ad_click,
  organic,
  ad_click_organic,
  search_with_ads_organic,
  monetizable_sap,
  dau_w_engine_as_default
FROM
  desktop_data_google
UNION ALL
SELECT
  submission_date,
  'Bing' AS partner,
  'desktop' AS device,
  NULL AS channel,
  country,
  dau,
  dau_engaged_w_sap,
  sap,
  tagged_sap,
  tagged_follow_on,
  search_with_ads,
  ad_click,
  organic,
  ad_click_organic,
  search_with_ads_organic,
  monetizable_sap,
  dau_w_engine_as_default
FROM
  desktop_data_bing
UNION ALL
SELECT
  submission_date,
  'DuckDuckGo' AS partner,
  'desktop' AS device,
  NULL AS channel,
  country,
  dau,
  ddg_dau_engaged_w_sap AS dau_engaged_w_sap,
  ddg_sap AS sap,
  ddg_tagged_sap AS tagged_sap,
  ddg_tagged_follow_on AS tagged_follow_on,
  ddg_search_with_ads AS search_with_ads,
  ddg_ad_click AS ad_click,
  ddg_organic AS organic,
  ddg_ad_click_organic AS ad_click_organic,
  ddg_search_with_ads_organic AS search_with_ads_organic,
  ddg_monetizable_sap AS monetizable_sap,
  ddg_dau_w_engine_as_default AS dau_w_engine_as_default
FROM
  desktop_data_ddg
UNION ALL
SELECT
  submission_date,
  'DuckDuckGo' AS partner,
  'extension' AS device,
  NULL AS channel,
  country,
  dau,
  ddgaddon_dau_engaged_w_sap AS dau_engaged_w_sap,
  ddgaddon_sap AS sap,
  ddgaddon_tagged_sap AS tagged_sap,
  ddgaddon_tagged_follow_on AS tagged_follow_on,
  ddgaddon_search_with_ads AS search_with_ads,
  ddgaddon_ad_click AS ad_click,
  ddgaddon_organic AS organic,
  ddgaddon_ad_click_organic AS ad_click_organic,
  ddgaddon_search_with_ads_organic AS search_with_ads_organic,
  ddgaddon_monetizable_sap AS monetizable_sap,
  ddgaddon_dau_w_engine_as_default AS dau_w_engine_as_default
FROM
  desktop_data_ddg
UNION ALL
SELECT
  submission_date,
  'Google' AS partner,
  'mobile' AS device,
  NULL AS channel,
  country,
  dau,
  dau_engaged_w_sap,
  sap,
  tagged_sap,
  tagged_follow_on,
  search_with_ads,
  ad_click,
  organic,
  ad_click_organic,
  search_with_ads_organic,
  monetizable_sap,
  dau_w_engine_as_default
FROM
  mobile_data_google
UNION ALL
SELECT
  submission_date,
  'Bing' AS partner,
  'mobile' AS device,
  NULL AS channel,
  country,
  dau,
  bing_dau_engaged_w_sap,
  bing_sap,
  bing_tagged_sap,
  bing_tagged_follow_on,
  bing_search_with_ads,
  bing_ad_click,
  bing_organic,
  bing_ad_click_organic,
  bing_search_with_ads_organic,
  bing_monetizable_sap,
  bing_dau_w_engine_as_default AS dau_w_engine_as_default
FROM
  mobile_data_bing_ddg
UNION ALL
SELECT
  submission_date,
  'DuckDuckGo' AS partner,
  'mobile' AS device,
  NULL AS channel,
  country,
  dau,
  ddg_dau_engaged_w_sap,
  ddg_sap,
  ddg_tagged_sap,
  ddg_tagged_follow_on,
  ddg_search_with_ads,
  ddg_ad_click,
  ddg_organic,
  ddg_ad_click_organic,
  ddg_search_with_ads_organic,
  ddg_monetizable_sap,
  ddg_dau_w_engine_as_default AS dau_w_engine_as_default
FROM
  mobile_data_bing_ddg
