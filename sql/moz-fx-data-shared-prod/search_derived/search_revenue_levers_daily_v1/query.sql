WITH desktop_data_google AS (
  SELECT
    submission_date,
    'Google' AS partner,
    'desktop' AS device,
    IF(LOWER(channel) LIKE '%esr%', 'ESR', 'personal') AS channel,
    country,
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
    `moz-fx-data-shared-prod.search.search_aggregates`
  WHERE
    submission_date = @submission_date
    AND (
      (submission_date < "2023-12-01" AND country NOT IN ('RU', 'UA', 'TR', 'BY', 'KZ', 'CN'))
      OR (submission_date >= "2023-12-01" AND country NOT IN ('RU', 'UA', 'BY', 'CN'))
    )
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
    country,
    'Bing' AS partner,
    'desktop' AS device,
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
    `moz-fx-data-shared-prod.search.search_aggregates`
  WHERE
    submission_date = @submission_date
    AND (distribution_id IS NULL OR distribution_id NOT LIKE '%acer%')
    AND NOT is_acer_cohort
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
    country,
    'DuckDuckGo' AS partner,
    'desktop' AS device,
    SUM(IF(engine IN ('ddg', 'duckduckgo'), sap, 0)) AS ddg_sap,
    SUM(IF(engine IN ('ddg', 'duckduckgo'), tagged_sap, 0)) AS ddg_tagged_sap,
    SUM(IF(engine IN ('ddg', 'duckduckgo'), tagged_follow_on, 0)) AS ddg_tagged_follow_on,
    SUM(IF(engine IN ('ddg', 'duckduckgo'), search_with_ads, 0)) AS ddg_search_with_ads,
    SUM(IF(engine IN ('ddg', 'duckduckgo'), ad_click, 0)) AS ddg_ad_click,
    SUM(IF(engine IN ('ddg', 'duckduckgo'), organic, 0)) AS ddg_organic,
    SUM(IF(engine IN ('ddg', 'duckduckgo'), ad_click_organic, 0)) AS ddg_ad_click_organic,
    SUM(
      IF(engine IN ('ddg', 'duckduckgo'), search_with_ads_organic, 0)
    ) AS ddg_search_with_ads_organic,
    SUM(IF(engine IN ('ddg', 'duckduckgo') AND is_sap_monetizable, sap, 0)) AS ddg_monetizable_sap,
    -- in-content probes not available for addon so these metrics although being here will be zero
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
    `moz-fx-data-shared-prod.search.search_aggregates`
  WHERE
    submission_date = @submission_date
  GROUP BY
    submission_date,
    country
  ORDER BY
    submission_date,
    country
),
-- -- Google Mobile (search only - as mobile search metrics is based on metrics
-- -- ping, while DAU should be based on main ping on Mobile, see also
-- -- https://mozilla-hub.atlassian.net/browse/RS-575)
mobile_data_google AS (
  SELECT
    submission_date,
    country,
    'Google' AS partner,
    'mobile' AS device,
    SUM(IF(normalized_engine = 'Google', search_count, 0)) AS sap,
    SUM(IF(normalized_engine = 'Google', tagged_sap, 0)) AS tagged_sap,
    SUM(IF(normalized_engine = 'Google', tagged_follow_on, 0)) AS tagged_follow_on,
    SUM(IF(normalized_engine = 'Google', search_with_ads, 0)) AS search_with_ads,
    SUM(IF(normalized_engine = 'Google', ad_click, 0)) AS ad_click,
    SUM(IF(normalized_engine = 'Google', organic, 0)) AS organic,
    SUM(IF(normalized_engine = 'Google', ad_click_organic, 0)) AS ad_click_organic,
    SUM(IF(normalized_engine = 'Google', search_with_ads_organic, 0)) AS search_with_ads_organic,
    -- metrics do not exist for mobile
    0 AS monetizable_sap
  FROM
    `moz-fx-data-shared-prod.search.mobile_search_aggregates`
  WHERE
    submission_date = @submission_date
    AND (
      (submission_date < "2023-12-01" AND country NOT IN ('RU', 'UA', 'TR', 'BY', 'KZ', 'CN'))
      OR (submission_date >= "2023-12-01" AND country NOT IN ('RU', 'UA', 'BY', 'CN'))
    )
    AND (
      app_name IN ('Fenix', 'Firefox Preview', 'Focus', 'Focus Android Glean', 'Focus iOS Glean')
      OR (app_name = 'Fennec' AND os = 'iOS')
    )
  GROUP BY
    submission_date,
    country
  ORDER BY
    submission_date,
    country
),
-- Bing & DDG Mobile (search only - as mobile search metrics is based on
-- metrics ping, while DAU should be based on main ping on Mobile, see also
-- https://mozilla-hub.atlassian.net/browse/RS-575)
mobile_data_bing AS (
  SELECT
    submission_date,
    country,
    'Bing' AS partner,
    'mobile' AS device,
    SUM(IF(normalized_engine = 'Bing', search_count, 0)) AS bing_sap,
    SUM(IF(normalized_engine = 'Bing', tagged_sap, 0)) AS bing_tagged_sap,
    SUM(IF(normalized_engine = 'Bing', tagged_follow_on, 0)) AS bing_tagged_follow_on,
    SUM(IF(normalized_engine = 'Bing', search_with_ads, 0)) AS bing_search_with_ads,
    SUM(IF(normalized_engine = 'Bing', ad_click, 0)) AS bing_ad_click,
    SUM(IF(normalized_engine = 'Bing', organic, 0)) AS bing_organic,
    SUM(IF(normalized_engine = 'Bing', ad_click_organic, 0)) AS bing_ad_click_organic,
    SUM(IF(normalized_engine = 'Bing', search_with_ads_organic, 0)) AS bing_search_with_ads_organic,
    -- metrics do not exist for mobile
    0 AS bing_monetizable_sap,
  FROM
    `moz-fx-data-shared-prod.search.mobile_search_aggregates`
  WHERE
    submission_date = @submission_date
    AND (
      app_name IN ('Fenix', 'Firefox Preview', 'Focus', 'Focus Android Glean', 'Focus iOS Glean')
      OR (app_name = 'Fennec' AND os = 'iOS')
    )
  GROUP BY
    submission_date,
    country
  ORDER BY
    submission_date,
    country
),
mobile_data_ddg AS (
  SELECT
    submission_date,
    country,
    'DuckDuckGo' AS partner,
    'mobile' AS device,
    SUM(IF(normalized_engine = 'DuckDuckGo', search_count, 0)) AS ddg_sap,
    SUM(IF(normalized_engine = 'DuckDuckGo', tagged_sap, 0)) AS ddg_tagged_sap,
    SUM(IF(normalized_engine = 'DuckDuckGo', tagged_follow_on, 0)) AS ddg_tagged_follow_on,
    SUM(IF(normalized_engine = 'DuckDuckGo', search_with_ads, 0)) AS ddg_search_with_ads,
    SUM(IF(normalized_engine = 'DuckDuckGo', ad_click, 0)) AS ddg_ad_click,
    SUM(IF(normalized_engine = 'DuckDuckGo', organic, 0)) AS ddg_organic,
    SUM(IF(normalized_engine = 'DuckDuckGo', ad_click_organic, 0)) AS ddg_ad_click_organic,
    SUM(
      IF(normalized_engine = 'DuckDuckGo', search_with_ads_organic, 0)
    ) AS ddg_search_with_ads_organic,
    -- metrics do not exist for mobile
    0 AS ddg_monetizable_sap
  FROM
    `moz-fx-data-shared-prod.search.mobile_search_aggregates`
  WHERE
    submission_date = @submission_date
    AND (
      app_name IN ('Fenix', 'Firefox Preview', 'Focus', 'Focus Android Glean', 'Focus iOS Glean')
      OR (app_name = 'Fennec' AND os = 'iOS')
    )
  GROUP BY
    submission_date,
    country
  ORDER BY
    submission_date,
    country
)
-- combine all desktop and mobile together
SELECT
  ddg.submission_date,
  ddg.partner,
  ddg.device,
  ddg.channel,
  ddg.country,
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
  desktop_data_google ddg
INNER JOIN
  `moz-fx-data-shared-prod.search.search_aggregates_dau` sad
  ON ddg.country = sad.country
  AND ddg.submission_date = sad.submission_date
  AND ddg.partner = sad.partner
  AND ddg.device = sad.device
UNION ALL
SELECT
  ddb.submission_date,
  ddb.partner,
  ddb.device,
  NULL AS channel,
  ddb.country,
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
  desktop_data_bing ddb
INNER JOIN
  `moz-fx-data-shared-prod.search.search_aggregates_dau` sad
  ON ddb.country = sad.country
  AND ddb.submission_date = sad.submission_date
  AND ddb.partner = sad.partner
  AND ddb.device = sad.device
UNION ALL
SELECT
  ddd.submission_date,
  ddd.partner,
  ddd.device,
  NULL AS channel,
  ddd.country,
  dau,
  dau_engaged_w_sap,
  ddg_sap AS sap,
  ddg_tagged_sap AS tagged_sap,
  ddg_tagged_follow_on AS tagged_follow_on,
  ddg_search_with_ads AS search_with_ads,
  ddg_ad_click AS ad_click,
  ddg_organic AS organic,
  ddg_ad_click_organic AS ad_click_organic,
  ddg_search_with_ads_organic AS search_with_ads_organic,
  ddg_monetizable_sap AS monetizable_sap,
  dau_w_engine_as_default
FROM
  desktop_data_ddg ddd
INNER JOIN
  `moz-fx-data-shared-prod.search.search_aggregates_dau` sad
  ON ddd.country = sad.country
  AND ddd.submission_date = sad.submission_date
  AND ddd.partner = sad.partner
  AND ddd.device = sad.device
UNION ALL
SELECT
  ddd.submission_date,
  ddd.partner,
  ddd.device,
  NULL AS channel,
  ddd.country,
  dau,
  dau_engaged_w_sap,
  ddgaddon_sap AS sap,
  ddgaddon_tagged_sap AS tagged_sap,
  ddgaddon_tagged_follow_on AS tagged_follow_on,
  ddgaddon_search_with_ads AS search_with_ads,
  ddgaddon_ad_click AS ad_click,
  ddgaddon_organic AS organic,
  ddgaddon_ad_click_organic AS ad_click_organic,
  ddgaddon_search_with_ads_organic AS search_with_ads_organic,
  ddgaddon_monetizable_sap AS monetizable_sap,
  dau_w_engine_as_default
FROM
  desktop_data_ddg ddd
INNER JOIN
  `moz-fx-data-shared-prod.search.search_aggregates_dau` sad
  ON ddd.country = sad.country
  AND ddd.submission_date = sad.submission_date
  AND ddd.partner = sad.partner
  AND ddd.device = sad.device
UNION ALL
SELECT
  mdg.submission_date,
  mdg.partner,
  mdg.device,
  'n/a' AS channel,
  mdg.country,
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
  mobile_data_google mdg
INNER JOIN
  `moz-fx-data-shared-prod.search.search_aggregates_dau` sad
  ON mdg.country = sad.country
  AND mdg.submission_date = sad.submission_date
  AND mdg.partner = sad.partner
  AND mdg.device = sad.device
UNION ALL
SELECT
  mdb.submission_date,
  mdb.partner,
  mdb.device,
  NULL AS channel,
  mdb.country,
  dau,
  dau_engaged_w_sap,
  bing_sap,
  bing_tagged_sap,
  bing_tagged_follow_on,
  bing_search_with_ads,
  bing_ad_click,
  bing_organic,
  bing_ad_click_organic,
  bing_search_with_ads_organic,
  bing_monetizable_sap,
  dau_w_engine_as_default
FROM
  mobile_data_bing mdb
INNER JOIN
  `moz-fx-data-shared-prod.search.search_aggregates_dau` sad
  ON mdb.country = sad.country
  AND mdb.submission_date = sad.submission_date
  AND mdb.partner = sad.partner
  AND mdb.device = sad.device
UNION ALL
SELECT
  mdd.submission_date,
  mdd.partner,
  mdd.device,
  NULL AS channel,
  mdd.country,
  dau,
  dau_engaged_w_sap,
  ddg_sap,
  ddg_tagged_sap,
  ddg_tagged_follow_on,
  ddg_search_with_ads,
  ddg_ad_click,
  ddg_organic,
  ddg_ad_click_organic,
  ddg_search_with_ads_organic,
  ddg_monetizable_sap,
  dau_w_engine_as_default
FROM
  mobile_data_ddg mdd
INNER JOIN
  `moz-fx-data-shared-prod.search.search_aggregates_dau` sad
  ON mdd.country = sad.country
  AND mdd.submission_date = sad.submission_date
  AND mdd.partner = sad.partner
  AND mdd.device = sad.device
