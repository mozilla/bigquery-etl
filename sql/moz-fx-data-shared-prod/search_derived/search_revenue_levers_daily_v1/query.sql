WITH

-- Grab Mobile Eligible DAU
mobile_dau_data AS (
  SELECT
    submission_date,
    SUM(
      IF(country NOT IN ("US", "RU", "UA", "BY", "TR", "KZ", "CN"), dau, 0)
    ) AS RoW_dau_eligible_google,
    SUM(IF(country = 'US', dau, 0)) AS US_dau_eligible_google,
    SUM(dau) AS dau
  FROM
    `mozdata.telemetry.active_users_aggregates_device`
  WHERE
    submission_date = @submission_date
    AND app_name IN ('Fenix', 'Firefox iOS', 'Focus Android', 'Focus Android')
  GROUP BY 1
)

-- Desktop
SELECT
  submission_date,
  normalized_engine AS partner,
  CASE
    WHEN normalized_engine = 'DuckDuckGo' THEN IF(engine IN ('ddg-addon'), 'extension', 'desktop')
    ELSE 'desktop'
    END AS device,
  CASE
    WHEN normalized_engine = 'Google' THEN IF(lower(channel) LIKE '%esr%', 'esr', 'personal')
    ELSE NULL
    END AS channel,
  CASE
    WHEN normalized_engine = 'Google' THEN IF(country = 'US', 'US', 'RoW')
    ELSE 'global'
    END AS country,
  COUNT(DISTINCT client_id) AS dau,
  COUNT(DISTINCT IF(sap > 0, client_id, NULL)) AS dau_engaged_w_sap,
  SUM(sap) AS sap,
  SUM(tagged_sap) AS tagged_sap,
  SUM(tagged_follow_on) AS tagged_follow_on,
  SUM(search_with_ads) AS search_with_ads,
  SUM(ad_click) AS ad_click
FROM `mozdata.search.search_clients_engines_sources_daily`
WHERE submission_date = @submission_date
AND (
  (normalized_engine = 'Google' AND country NOT IN ("RU", "UA", "TR", "BY", "KZ", "CN"))
  OR (normalized_engine = 'Bing' AND distribution_id NOT LIKE '%acer%')
  OR (normalized_engine = 'DuckDuckGo' AND engine IN ('ddg', 'duckduckgo', 'ddg-addon'))
)
GROUP BY
  1,
  2,
  3,
  4,
  5
ORDER BY
  1,
  2,
  3,
  4,
  5

UNION ALL

-- Mobile (search only - as mobile search metrics is based on metrics ping, while DAU should be based on main ping on Mobile, also see here also see https://mozilla-hub.atlassian.net/browse/RS-575)
SELECT
  submission_date,
  normalized_engine AS partner,
  'mobile' AS device,
  NULL AS channel,,
  CASE
    WHEN normalized_engine = 'Google' THEN IF(country = 'US', 'US', 'RoW')
    ELSE 'global'
    END AS country,
  -- COUNT(distinct client_id) as dau, --should avoid as mentioned in above
  CASE
    WHEN normalized_engine = 'Google' THEN IF(country = 'US', US_dau_eligible_google, RoW_dau_eligible_google)
    ELSE dau
    END AS dau,
  COUNT(DISTINCT IF(sap > 0, client_id, NULL)) AS dau_engaged_w_sap,
  SUM(sap) AS sap,
  SUM(tagged_sap) AS tagged_sap,
  SUM(tagged_follow_on) AS tagged_follow_on,
  SUM(search_with_ads) AS search_with_ads,
  SUM(ad_click) AS ad_click
FROM
  `mozdata.search.mobile_search_clients_engines_sources_daily`
INNER JOIN
  mobile_dau_data
USING
  (submission_date)
WHERE
  submission_date = @submission_date
  AND (
    (normalized_engine = 'Google' AND country NOT IN ("RU", "UA", "BY", "TR", "KZ", "CN"))
    OR (normalized_engine = 'Bing')
    OR (normalized_engine = 'DuckDuckGo')
  )
  AND app_name IN ("Focus", "Fenix", "Fennec")
GROUP BY
  1,
  2,
  3,
  4
ORDER BY
  1,
  2,
  3,
  4
