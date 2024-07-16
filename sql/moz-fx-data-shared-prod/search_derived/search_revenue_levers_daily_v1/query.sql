-- selects DESKTOP search telemetry from Google, Bing, DDG default engines
-- we need all clients by default engine for DAU counting
-- for search reporting we only want searches by normalized_engine
WITH desktop_search_telemetry AS (
  SELECT
    submission_date,
    country,
    client_id,
    default_search_engine,
    normalized_engine,
    -- differentiate Google ESR and DDG extension via normalized_channel
    -- IF((normalized_engine = 'Google') AND (LOWER(channel) LIKE '%esr%'), 'ESR',
    --   IF(engine IN ('ddg-addon'), "extension",
    --   'personal')) AS normalized_channel,
    SUM(sap) AS sap,
    SUM(tagged_sap) AS tagged_sap,
    SUM(tagged_follow_on) AS tagged_follow_on,
    SUM(search_with_ads) AS search_with_ads,
    SUM(ad_click) AS ad_click,
    SUM(organic) AS organic,
    SUM(ad_click_organic) AS ad_click_organic,
    SUM(search_with_ads_organic) AS search_with_ads_organic,
    SUM(IF(is_sap_monetizable, sap, 0)) AS monetizable_sap
  FROM
    `moz-fx-data-shared-prod.search.search_clients_engines_sources_daily`
  WHERE
    submission_date = @submission_date
    AND (
      -- Google has specific market restrictions
      (
        (default_search_engine LIKE '%google%' OR normalized_engine = "Google")
        AND (
          (submission_date < "2023-12-01" AND country NOT IN ('RU', 'UA', 'TR', 'BY', 'KZ', 'CN'))
          OR (submission_date >= "2023-12-01" AND country NOT IN ('RU', 'UA', 'BY', 'CN'))
        )
      )
      -- Bing removes Acer distribution
      OR (
        (default_search_engine LIKE '%bing%' OR normalized_engine = "Bing")
        AND (
          (distribution_id IS NULL OR distribution_id NOT LIKE '%acer%')
          AND client_id NOT IN (SELECT client_id FROM `moz-fx-data-shared-prod.search.acer_cohort`)
        )
      )
          -- DDG has no additional filters
      OR (
        default_search_engine LIKE '%ddg%'
        OR default_search_engine LIKE '%duckduckgo%'
        OR default_search_engine LIKE 'ddg%addon'
        OR normalized_engine = "DuckDuckGo"
      )
    )
  GROUP BY
    submission_date,
    country,
    default_search_engine,
    normalized_engine,
    -- normalized_channel,
    client_id
  ORDER BY
    submission_date,
    country,
    default_search_engine,
    normalized_engine,
    -- normalized_channel,
    client_id
),
-- selects MOBILE search telemetry from Google, Bing, DDG default engines
-- we need all clients by default engine for DAU counting
-- for search reporting we only want searches by normalized_engine
mobile_search_telemetry AS (
  SELECT
    submission_date,
    country,
    default_search_engine,
    normalized_engine,
    -- "personal" as normalized_channel,
    client_id,
    SUM(sap) AS sap,
    SUM(tagged_sap) AS tagged_sap,
    SUM(tagged_follow_on) AS tagged_follow_on,
    SUM(search_with_ads) AS search_with_ads,
    SUM(ad_click) AS ad_click,
    SUM(organic) AS organic,
    SUM(ad_click_organic) AS ad_click_organic,
    SUM(search_with_ads_organic) AS search_with_ads_organic,
    0 AS monetizable_sap -- does not exist for mobile
  FROM
    `moz-fx-data-shared-prod.search.mobile_search_clients_engines_sources_daily`
  WHERE
    submission_date = @submission_date
    AND (
      -- Google has specific market restrictions
      (
        (default_search_engine LIKE '%google%' OR normalized_engine = "Google")
        AND (
          (submission_date < "2023-12-01" AND country NOT IN ('RU', 'UA', 'TR', 'BY', 'KZ', 'CN'))
          OR (submission_date >= "2023-12-01" AND country NOT IN ('RU', 'UA', 'BY', 'CN'))
        )
      )
      -- Bing has no additional filters
      OR ((default_search_engine LIKE '%bing%' OR normalized_engine = "Bing"))
      -- DDG has no additional filters
      OR (
        default_search_engine LIKE '%ddg%'
        OR default_search_engine LIKE '%duckduckgo%'
        OR default_search_engine LIKE 'ddg%addon'
        OR normalized_engine = "DuckDuckGo"
      )
    )
  GROUP BY
    submission_date,
    country,
    default_search_engine,
    normalized_engine,
    -- normalized_channel,
    client_id
  ORDER BY
    submission_date,
    country,
    default_search_engine,
    normalized_engine,
    -- normalized_channel,
    client_id
),
search_telemetry AS (
  SELECT
    "desktop" AS device,
    submission_date,
    country,
    client_id,
    default_search_engine,
    normalized_engine,
    -- normalized_channel,
    sap,
    tagged_sap,
    tagged_follow_on,
    search_with_ads,
    ad_click,
    organic,
    ad_click_organic,
    search_with_ads_organic,
    monetizable_sap
  FROM
    desktop_search_telemetry
  UNION ALL
  SELECT
    "mobile" AS device,
    submission_date,
    country,
    client_id,
    default_search_engine,
    normalized_engine,
    -- normalized_channel,
    sap,
    tagged_sap,
    tagged_follow_on,
    search_with_ads,
    ad_click,
    organic,
    ad_click_organic,
    search_with_ads_organic,
    monetizable_sap
  FROM
    mobile_search_telemetry
),
-- we don't restrict search metrics to KPI-eligible clients
-- we report all activity from top search engines
search_metrics AS (
  SELECT
    device,
    submission_date,
    country,
    normalized_engine,
      --  normalized_channel,
    SUM(sap) AS sap,
    SUM(tagged_sap) AS tagged_sap,
    SUM(tagged_follow_on) AS tagged_follow_on,
    SUM(search_with_ads) AS search_with_ads,
    SUM(ad_click) AS ad_click,
    SUM(organic) AS organic,
    SUM(ad_click_organic) AS ad_click_organic,
    SUM(search_with_ads_organic) AS search_with_ads_organic,
    SUM(monetizable_sap) AS monetizable_sap
  FROM
    search_telemetry
  WHERE
    normalized_engine IN ("Google", "Bing", "DuckDuckGo")
  GROUP BY
    submission_date,
    device,
    country,
    normalized_engine
      --  normalized_channel
),
-- dau from active users tables
-- all activity filtering done behind the scenes, managed by browsers DS
-- use individual device tables since will need to filter by country
browsers_dau AS (
  SELECT
    submission_date,
    "desktop" AS device,
    country,
    client_id
  FROM
    `mozdata.telemetry.desktop_active_users`
  WHERE
    submission_date = @submission_date
    AND is_dau
  UNION ALL
  SELECT
    submission_date,
    "mobile" AS device,
    country,
    client_id
  FROM
    `mozdata.telemetry.mobile_active_users`
  WHERE
    submission_date = @submission_date
    AND is_dau
),
-- merge search & DAU data to calculate search DAU
-- no group by normalized_engine since want default search engine regardless
dau_metrics_wide AS (
  SELECT
    submission_date,
    device,
    country,
    COUNT(DISTINCT client_id) AS dau_eligible_markets,
    COUNT(
      DISTINCT IF(
        (
          default_search_engine LIKE '%google%'
          AND (
            (submission_date < "2023-12-01" AND country NOT IN ('RU', 'UA', 'TR', 'BY', 'KZ', 'CN'))
            OR (submission_date >= "2023-12-01" AND country NOT IN ('RU', 'UA', 'BY', 'CN'))
          )
        ),
        client_id,
        NULL
      )
    ) AS google_dau_w_engine_as_default,
    COUNT(
      DISTINCT IF(
        (
          default_search_engine LIKE '%bing%'
          -- AND ((distribution_id IS NULL OR distribution_id NOT LIKE '%acer%')
          AND client_id NOT IN (SELECT client_id FROM `moz-fx-data-shared-prod.search.acer_cohort`)
        ),
        client_id,
        NULL
      )
    ) AS bing_dau_w_engine_as_default,
    COUNT(
      DISTINCT IF(
        (
          (
            default_search_engine LIKE '%ddg%'
            OR default_search_engine LIKE '%duckduckgo%'
            OR default_search_engine LIKE 'ddg%addon'
          )
        ),
        client_id,
        NULL
      )
    ) AS ddg_dau_w_engine_as_default,
    COUNT(
      DISTINCT IF(
        sap > 0
        AND (
          (
            normalized_engine = 'Google'
            AND default_search_engine LIKE '%google%'
            AND (
              (
                submission_date < "2023-12-01"
                AND country NOT IN ('RU', 'UA', 'TR', 'BY', 'KZ', 'CN')
              )
              OR (submission_date >= "2023-12-01" AND country NOT IN ('RU', 'UA', 'BY', 'CN'))
            )
          )
        ),
        client_id,
        NULL
      )
    ) AS google_dau_engaged_w_sap,
    COUNT(
      DISTINCT IF(
        sap > 0
        AND (
          normalized_engine = 'Bing'
          AND default_search_engine LIKE '%bing%'
            -- AND ((distribution_id IS NULL OR distribution_id NOT LIKE '%acer%')
          AND client_id NOT IN (SELECT client_id FROM `moz-fx-data-shared-prod.search.acer_cohort`)
        ),
        client_id,
        NULL
      )
    ) AS bing_dau_engaged_w_sap,
    COUNT(
      DISTINCT IF(
        sap > 0
        AND (
          normalized_engine = 'DuckDuckGo'
          AND (
            default_search_engine LIKE '%ddg%'
            OR default_search_engine LIKE '%duckduckgo%'
            OR default_search_engine LIKE 'ddg%addon'
          )
        ),
        client_id,
        NULL
      )
    ) AS ddg_dau_engaged_w_sap,
  FROM
    browsers_dau
  FULL JOIN
    search_telemetry
    USING (submission_date, client_id, country, device)
  GROUP BY
    submission_date,
    device,
    country
      --  normalized_engine
      --  normalized_channel
),
dau_metrics_long AS (
  SELECT
    submission_date,
    device,
    country,
    "Google" AS normalized_engine,
    dau_eligible_markets,
    google_dau_w_engine_as_default AS dau_w_engine_as_default,
    google_dau_engaged_w_sap AS dau_engaged_w_sap,
  FROM
    dau_metrics_wide
  UNION ALL
  SELECT
    submission_date,
    device,
    country,
    "Bing" AS normalized_engine,
    dau_eligible_markets,
    bing_dau_w_engine_as_default AS dau_w_engine_as_default,
    bing_dau_engaged_w_sap AS dau_engaged_w_sap,
  FROM
    dau_metrics_wide
  UNION ALL
  SELECT
    submission_date,
    device,
    country,
    "DuckDuckGo" AS normalized_engine,
    dau_eligible_markets,
    ddg_dau_w_engine_as_default AS dau_w_engine_as_default,
    ddg_dau_engaged_w_sap AS dau_engaged_w_sap,
  FROM
    dau_metrics_wide
)
SELECT
  submission_date,
  device,
  country,
  normalized_engine,
  dau_eligible_markets,
  dau_w_engine_as_default,
  dau_engaged_w_sap,
  sap,
  tagged_sap,
  tagged_follow_on,
  search_with_ads,
  ad_click,
  organic,
  ad_click_organic,
  search_with_ads_organic,
  monetizable_sap
FROM
  search_metrics
LEFT JOIN
  dau_metrics_long
  USING (submission_date, device, country, normalized_engine)
