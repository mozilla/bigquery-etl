WITH

-- Google Desktop (search + DAU)
desktop_data_google AS (
  SELECT
    submission_date,
    IF(lower(channel) LIKE '%esr%', 'esr', 'personal') AS channel,
    IF(country = 'US', 'US', 'RoW') AS country,
    count(DISTINCT client_id) AS dau,
    count(
      DISTINCT IF(sap > 0 AND normalized_engine = 'Google', client_id, NULL)
    ) AS dau_engaged_w_sap,
    sum(IF(normalized_engine = 'Google', sap, 0)) AS sap,
    sum(IF(normalized_engine = 'Google', tagged_sap, 0)) AS tagged_sap,
    sum(IF(normalized_engine = 'Google', tagged_follow_on, 0)) AS tagged_follow_on,
    sum(IF(normalized_engine = 'Google', search_with_ads, 0)) AS search_with_ads,
    sum(IF(normalized_engine = 'Google', ad_click, 0)) AS ad_click
  FROM
    `mozdata.search.search_clients_engines_sources_daily`
  WHERE
    submission_date = @submission_date
    AND country NOT IN ('RU', 'UA', 'TR', 'BY', 'KZ', 'CN')
  GROUP BY
    1,
    2,
    3
  ORDER BY
    1,
    2,
    3
),

-- Bing Desktop (non-Acer)
desktop_data_bing AS (
  SELECT
    submission_date,
    count(DISTINCT client_id) AS dau,
    count(
      DISTINCT IF(sap > 0 AND normalized_engine = 'Bing', client_id, NULL)
    ) AS dau_engaged_w_sap,
    sum(IF(normalized_engine = 'Bing', sap, 0)) AS sap,
    sum(IF(normalized_engine = 'Bing', tagged_sap, 0)) AS tagged_sap,
    sum(IF(normalized_engine = 'Bing', tagged_follow_on, 0)) AS tagged_follow_on,
    sum(IF(normalized_engine = 'Bing', search_with_ads, 0)) AS search_with_ads,
    sum(IF(normalized_engine = 'Bing', ad_click, 0)) AS ad_click
  FROM
    `mozdata.search.search_clients_engines_sources_daily`
  WHERE
    submission_date = @submission_date
    AND client_id NOT IN (SELECT client_id FROM `mozdata.analysis.acer_cohort`)
  GROUP BY
    1
  ORDER BY
    1
),

-- DDG Desktop + Extension
desktop_data_ddg AS (
  SELECT
    submission_date,
    count(DISTINCT client_id) AS dau,
    count(DISTINCT IF((engine) IN ('ddg', 'duckduckgo') AND sap > 0, client_id, NULL)) AS ddg_dau_engaged_w_sap,
    sum(IF(engine IN ('ddg', 'duckduckgo'), sap, 0)) AS ddg_sap,
    sum(IF(engine IN ('ddg', 'duckduckgo'), tagged_sap, 0)) AS ddg_tagged_sap,
    sum(IF(engine IN ('ddg', 'duckduckgo'), tagged_sap, 0)) AS ddg_tagged_follow_on,
    sum(IF(engine IN ('ddg', 'duckduckgo'), search_with_ads, 0)) AS ddg_search_with_ads,
    sum(IF(engine IN ('ddg', 'duckduckgo'), ad_click, 0)) AS ddg_ad_click,
    -- in-content probes not available for addon so these metrics although being here will be zero
    count(DISTINCT IF(engine = 'ddg-addon' AND sap > 0, client_id, NULL)) AS ddgaddon_dau_engaged_w_sap,
    sum(IF(engine IN ('ddg-addon'), sap, 0)) AS ddgaddon_sap,
    sum(IF(engine IN ('ddg-addon'), tagged_sap, 0)) AS ddgaddon_tagged_sap,
    sum(IF(engine IN ('ddg-addon'), tagged_sap, 0)) AS ddgaddon_tagged_follow_on,
    sum(IF(engine IN ('ddg-addon'), search_with_ads, 0)) AS ddgaddon_search_with_ads,
    sum(IF(engine IN ('ddg-addon'), ad_click, 0)) AS ddgaddon_ad_click
  FROM
    `mozdata.search.search_clients_engines_sources_daily`
  WHERE
    submission_date = @submission_date
  GROUP BY
    1
  ORDER BY
    1
),

-- Grab Mobile Eligible DAU
mobile_dau_data AS (
  SELECT
    submission_date,
    sum(
      IF(country NOT IN ('US', 'RU', 'UA', 'BY', 'TR', 'KZ', 'CN'), dau, 0)
    ) AS RoW_dau_eligible_google,
    sum(IF(country = 'US', dau, 0)) AS US_dau_eligible_google,
    sum(dau) AS dau
  FROM
    `mozdata.telemetry.active_users_aggregates_device`
  WHERE
    submission_date = @submission_date
    AND app_name IN ('Fenix', 'Firefox iOS', 'Focus Android', 'Focus Android')
  GROUP BY 1
),

-- Google Mobile (search only - as mobile search metrics is based on metrics
-- ping, while DAU should be based on main ping on Mobile, also see here also
-- see https://mozilla-hub.atlassian.net/browse/RS-575)
mobile_data_google AS (
  SELECT
    submission_date,
    IF(country = 'US', 'US', 'RoW') AS country,
    -- count(distinct client_id) as dau, --should avoid as mentioned in above
    IF(country = 'US', dau.US_dau_eligible_google, dau.RoW_dau_eligible_google) AS dau,
    count(
      DISTINCT IF(sap > 0 AND normalized_engine = 'Google', client_id, NULL)
    ) AS dau_engaged_w_sap,
    sum(IF(normalized_engine = 'Google', sap, 0)) AS sap,
    sum(IF(normalized_engine = 'Google', tagged_sap, 0)) AS tagged_sap,
    sum(IF(normalized_engine = 'Google', tagged_follow_on, 0)) AS tagged_follow_on,
    sum(IF(normalized_engine = 'Google', search_with_ads, 0)) AS search_with_ads,
    sum(IF(normalized_engine = 'Google', ad_click, 0)) AS ad_click
  FROM
    `mozdata.search.mobile_search_clients_engines_sources_daily`
  INNER JOIN
    mobile_dau_data dau
  USING
    (submission_date)
  WHERE
    submission_date = @submission_date
    AND country NOT IN ('RU', 'UA', 'BY', 'TR', 'KZ', 'CN')
    AND app_name IN ('Focus', 'Fenix', 'Fennec')
  GROUP BY
    1,
    2,
    3
  ORDER BY
    1,
    2,
    3
),

-- Bing & DDG Mobile (search only - as mobile search metrics is based on
-- metrics ping, while DAU should be based on main ping on Mobile, also see
-- here also see https://mozilla-hub.atlassian.net/browse/RS-575)
mobile_data_bing_ddg AS (
  SELECT
    submission_date,
    -- count(distinct client_id) as dau, --should avoid as mentioned in above
    dau.dau,
    count(
      DISTINCT IF(sap > 0 AND normalized_engine = 'Bing', client_id, NULL)
    ) AS bing_dau_engaged_w_sap,
    count(
      DISTINCT IF(sap > 0 AND normalized_engine = 'DuckDuckGo', client_id, NULL)
    ) AS ddg_dau_engaged_w_sap,
    sum(IF(normalized_engine = 'Bing', sap, 0)) AS bing_sap,
    sum(IF(normalized_engine = 'Bing', tagged_sap, 0)) AS bing_tagged_sap,
    sum(IF(normalized_engine = 'Bing', tagged_follow_on, 0)) AS bing_tagged_follow_on,
    sum(IF(normalized_engine = 'Bing', search_with_ads, 0)) AS bing_search_with_ads,
    sum(IF(normalized_engine = 'Bing', ad_click, 0)) AS bing_ad_click,
    sum(IF(normalized_engine = 'DuckDuckGo', sap, 0)) AS ddg_sap,
    sum(IF(normalized_engine = 'DuckDuckGo', tagged_sap, 0)) AS ddg_tagged_sap,
    sum(IF(normalized_engine = 'DuckDuckGo', tagged_follow_on, 0)) AS ddg_tagged_follow_on,
    sum(IF(normalized_engine = 'DuckDuckGo', search_with_ads, 0)) AS ddg_search_with_ads,
    sum(IF(normalized_engine = 'DuckDuckGo', ad_click, 0)) AS ddg_ad_click
  FROM
    `mozdata.search.mobile_search_clients_engines_sources_daily`
  INNER JOIN
    mobile_dau_data dau
  USING
    (submission_date)
  WHERE
    submission_date = @submission_date
    AND app_name IN ('Focus', 'Fenix', 'Fennec')
  GROUP BY
    1,
    2
  ORDER BY
    1,
    2
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
  ad_click
FROM
  desktop_data_google

UNION ALL

SELECT
  submission_date,
  'Bing' AS partner,
  'desktop' AS device,
  NULL AS channel,
  'global' AS country,
  dau,
  dau_engaged_w_sap,
  sap,
  tagged_sap,
  tagged_follow_on,
  ad_click
FROM
  desktop_data_bing

UNION ALL

SELECT
  submission_date,
  'DuckDuckGo' AS partner,
  'desktop' AS device,
  NULL AS channel,
  'global' AS country,
  dau,
  ddg_dau_engaged_w_sap AS dau_engaged_w_sap,
  ddg_sap AS sap,
  ddg_tagged_sap AS tagged_sap,
  ddg_tagged_follow_on AS tagged_follow_on,
  ddg_ad_click AS ad_click
FROM
  desktop_data_ddg

UNION ALL

SELECT
  submission_date,
  'DuckDuckGo' AS partner,
  'extension' AS device,
  NULL AS channel,
  'global' AS country,
  dau,
  ddgaddon_dau_engaged_w_sap AS dau_engaged_w_sap,
  ddgaddon_sap AS sap,
  ddgaddon_tagged_sap AS tagged_sap,
  ddgaddon_tagged_follow_on AS tagged_follow_on,
  ddgaddon_ad_click AS ad_click
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
  ad_click
FROM
  mobile_data_google

UNION ALL

SELECT
  submission_date,
  'Bing' AS partner,
  'mobile' AS device,
  NULL AS channel,
  'global' AS country,
  dau,
  bing_dau_engaged_w_sap,
  bing_sap,
  bing_tagged_sap,
  bing_tagged_follow_on,
  bing_ad_click
FROM
  mobile_data_bing_ddg

UNION ALL

SELECT
  submission_date,
  'DuckDuckGo' AS partner,
  'mobile' AS device,
  NULL AS channel,
  'global' AS country,
  dau,
  ddg_dau_engaged_w_sap,
  ddg_sap,
  ddg_tagged_sap,
  ddg_tagged_follow_on,
  ddg_ad_click
FROM
  mobile_data_bing_ddg
