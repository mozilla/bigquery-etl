WITH
## Google Desktop (search + DOU)
desktop_data_google AS (
  SELECT
    submission_date,
    IF(lower(channel) LIKE '%esr%', 'esr', 'personal') AS channel,
    IF(country = 'US', 'US', 'RoW') AS country,
    count(DISTINCT client_id) AS dou,
    count(
      DISTINCT IF(sap > 0 AND normalized_engine = 'Google', client_id, NULL)
    ) AS dou_engaged_w_sap,
    sum(IF(normalized_engine = 'Google', sap, 0)) AS sap,
    sum(IF(normalized_engine = 'Google', tagged_sap, 0)) AS tagged_sap,
    sum(IF(normalized_engine = 'Google', tagged_follow_on, 0)) AS tagged_follow_on,
    sum(IF(normalized_engine = 'Google', search_with_ads, 0)) AS search_with_ads,
    sum(IF(normalized_engine = 'Google', ad_click, 0)) AS ad_click
  FROM
    `mozdata.search.search_clients_engines_sources_daily`
  WHERE
    submission_date = @submission_date
    AND country NOT IN ("RU", "UA", "TR", "BY", "KZ", "CN")
  GROUP BY
    1,
    2,
    3
  ORDER BY
    1,
    2,
    3
),
## Bing Desktop (non-Acer)
desktop_data_bing AS (
  SELECT
    submission_date,
    count(DISTINCT client_id) AS dou,
    count(
      DISTINCT IF(sap > 0 AND normalized_engine = 'Bing', client_id, NULL)
    ) AS dou_engaged_w_sap,
    sum(IF(normalized_engine = 'Bing', sap, 0)) AS sap,
    sum(IF(normalized_engine = 'Bing', tagged_sap, 0)) AS tagged_sap,
    sum(IF(normalized_engine = 'Bing', tagged_follow_on, 0)) AS tagged_follow_on,
    sum(IF(normalized_engine = 'Bing', search_with_ads, 0)) AS search_with_ads,
    sum(IF(normalized_engine = 'Bing', ad_click, 0)) AS ad_click
  FROM
    `mozdata.search.search_clients_engines_sources_daily`
  WHERE
    submission_date = @submission_date
    AND distribution_id NOT LIKE '%acer%'
  GROUP BY
    1,
    2,
    3
  ORDER BY
    1,
    2,
    3
),
## DDG Desktop + Extension
desktop_data_ddg AS (
  SELECT
    submission_date,
    count(DISTINCT client_id) AS dou,
    count(DISTINCT IF((engine) IN ("ddg", 'duckduckgo') AND sap > 0, client_id, NULL)) AS ddg_adou,
    sum(IF(engine IN ('ddg', 'duckduckgo'), sap, 0)) AS ddg_sap,
    sum(IF(engine IN ('ddg', 'duckduckgo'), tagged_sap, 0)) AS ddg_tagged_sap,
    sum(IF(engine IN ('ddg', 'duckduckgo'), tagged_sap, 0)) AS ddg_tagged_follow_on,
    sum(IF(engine IN ('ddg', 'duckduckgo'), search_with_ads, 0)) AS ddg_search_with_ads,
    sum(IF(engine IN ('ddg', 'duckduckgo'), ad_click, 0)) AS ddg_adclick,
    # in-content probes not available for addon so these metrics although being here will be zero
    count(DISTINCT IF(engine = 'ddg-addon' AND sap > 0, client_id, NULL)) AS ddgaddon_adou,
    sum(IF(engine IN ('ddg-addon'), sap, 0)) AS ddgaddon_sap,
    sum(IF(engine IN ('ddg-addon'), tagged_sap, 0)) AS ddgaddon_tagged_sap,
    sum(IF(engine IN ('ddg-addon'), tagged_sap, 0)) AS ddgaddon_tagged_follow_on,
    sum(IF(engine IN ('ddg-addon'), search_with_ads, 0)) AS ddgaddon_search_with_ads,
    sum(IF(engine IN ('ddg-addon'), ad_click, 0)) AS ddgaddon_adclick
  FROM
    `mozdata.search.search_clients_engines_sources_daily`
  WHERE
    submission_date = @submission_date
  GROUP BY
    1,
    2,
    3
  ORDER BY
    1,
    2,
    3
),
## Grab Mobile Eligible DOU
mobile_dou_data AS (
  SELECT
    submission_date,
    sum(
      IF(country NOT IN ("US", "RU", "UA", "BY", "TR", "KZ", "CN"), dau, 0)
    ) AS RoW_dou_eligible_google,
    sum(IF(country = 'US', dau, 0)) AS US_dou_eligible_google,
    sum(dau) AS dou
  FROM
    `mozdata.telemetry.active_users_aggregates_device`
  WHERE
    submission_date = @submission_date
    AND app_name IN ('Fenix', 'Firefox iOS', 'Focus Android', 'Focus Android')
),
## Google Mobile (search only - as mobile search metrics is based on metrics ping, while DOU should be based on main ping on Mobile, also see here also see https://mozilla-hub.atlassian.net/browse/RS-575)
mobile_search_data_google AS (
  SELECT
    submission_date,
    IF(country = 'US', 'US', 'RoW') AS country,
    # count(distinct client_id) as dou, --should avoid as mentioned in above
    count(
      DISTINCT IF(sap > 0 AND normalized_engine = 'Google', client_id, NULL)
    ) AS dou_engaged_w_sap,
    sum(IF(normalized_engine = 'Google', sap, 0)) AS sap,
    sum(IF(normalized_engine = 'Google', tagged_sap, 0)) AS tagged_sap,
    sum(IF(normalized_engine = 'Google', tagged_follow_on, 0)) AS tagged_follow_on,
    sum(IF(normalized_engine = 'Google', search_with_ads, 0)) AS search_with_ads,
    sum(IF(normalized_engine = 'Google', ad_click, 0)) AS ad_click
  FROM
    `mozdata.search.mobile_search_clients_engines_sources_daily`
  WHERE
    submission_date = @submission_date
    AND country NOT IN ("RU", "UA", "BY", "TR", "KZ", "CN")
    AND app_name IN ("Focus", "Fenix", "Fennec")
  GROUP BY
    1,
    2
  ORDER BY
    1,
    2
),
mobile_data_google AS (
  SELECT
    submission_date,
    country,
    IF(country = 'US', US_dou_eligible_google, RoW_dou_eligible_google) AS dou,
    dou_engaged_w_sap,
    sap,
    tagged_sap,
    tagged_follow_on,
    search_with_ads,
    ad_click
  FROM
    mobile_search_data_google
  INNER JOIN
    mobile_dou_data
  USING
    (submission_date)
),
## Bing & DDG Mobile (search only - as mobile search metrics is based on metrics ping, while DOU should be based on main ping on Mobile, also see here also see https://mozilla-hub.atlassian.net/browse/RS-575)
mobile_search_data_bing_ddg AS (
  SELECT
    submission_date,
    # count(distinct client_id) as dou, --should avoid as mentioned in above
    count(
      DISTINCT IF(sap > 0 AND normalized_engine = 'Bing', client_id, NULL)
    ) AS bing_dou_engaged_w_sap,
    count(
      DISTINCT IF(sap > 0 AND normalized_engine = 'DuckDuckGo', client_id, NULL)
    ) AS ddg_dou_engaged_w_sap,
    sum(IF(normalized_engine = 'Bing', sap, 0)) AS bing_sap,
    sum(IF(normalized_engine = 'Bing', tagged_sap, 0)) AS bing_tagged_sap,
    sum(IF(normalized_engine = 'Bing', tagged_follow_on, 0)) AS bing_tagged_follow_on,
    sum(IF(normalized_engine = 'Bing', search_with_ads, 0)) AS bing_search_with_ads,
    sum(IF(normalized_engine = 'Bing', ad_click, 0)) AS bing_bing_ad_click,
    sum(IF(normalized_engine = 'DuckDuckGo', sap, 0)) AS ddg_sap,
    sum(IF(normalized_engine = 'DuckDuckGo', tagged_sap, 0)) AS ddg_tagged_sap,
    sum(IF(normalized_engine = 'DuckDuckGo', tagged_follow_on, 0)) AS ddg_tagged_follow_on,
    sum(IF(normalized_engine = 'DuckDuckGo', search_with_ads, 0)) AS ddg_search_with_ads,
    sum(IF(normalized_engine = 'DuckDuckGo', ad_click, 0)) AS ddg_ad_click
  FROM
    `mozdata.search.mobile_search_clients_engines_sources_daily`
  WHERE
    submission_date = @submission_date
    AND app_name IN ("Focus", "Fenix", "Fennec")
  GROUP BY
    1
  ORDER BY
    1
),
mobile_data_bing AS (
  SELECT
    submission_date,
    dou,
    bing_dou_engaged_w_sap AS dou_engaged_w_sap,
    bing_sap AS sap,
    bing_tagged_sap AS tagged_sap,
    bing_tagged_follow_on AS tagged_follow_on,
    bing_search_with_ads AS search_with_ads,
    bing_ad_click AS ad_click
  FROM
    mobile_search_data_bing_ddg
  INNER JOIN
    mobile_dou_data
  USING
    (submission_date)
),
mobile_data_ddg AS (
  SELECT
    submission_date,
    dou,
    ddg_dou_engaged_w_sap AS dou_engaged_w_sap,
    ddg_sap AS sap,
    ddg_tagged_sap AS tagged_sap,
    ddg_tagged_follow_on AS tagged_follow_on,
    ddg_serach_with_ads AS search_with_ads,
    ddg_ad_click AS ad_click
  FROM
    mobile_search_data_bing_ddg
  INNER JOIN
    mobile_dou_data
  USING
    (submission_date)
),
# combine all desktop and mobile together
SELECT
  submission_date,
  partner,
  device,
  channel,
  country,
  dou,
  adou,
  sap,
  ad_click
FROM
  (
    SELECT
      submission_date,
      "Google" AS partner,
      country,
      'desktop' AS device,
      channel,
      dou,
      dou_engaged_w_sap,
      sap,
      tagged_sap,
      tagged_follow_on,
      ad_click
    FROM
      desktop_data_google
  )
UNION ALL
  (
    SELECT
      submission_date,
      "Bing" AS partner,
      'global' AS country,
      'desktop' AS device,
      NULL AS channel,
      dou,
      dou_engaged_w_sap,
      sap,
      tagged_sap,
      tagged_follow_on,
      ad_click
    FROM
      desktop_data_bing
  )
UNION ALL
  (
    SELECT
      submission_date,
      "DuckDuckGo" AS partner,
      'global' AS country,
      'desktop' AS device,
      NULL AS channel,
      dou,
      ddg_dou_engaged_w_sap AS dou_engaged_w_sap,
      ddg_sap AS sap,
      ddg_tagged_sap AS tagged_sap,
      ddg_tagged_follow_on AS tagged_follow_on,
      ddg_ad_click AS ad_click
    FROM
      desktop_data_ddg
  )
UNION ALL
  (
    SELECT
      submission_date,
      "DuckDuckGo" AS partner,
      'global' AS country,
      'extension' AS device,
      NULL AS channel,
      dou,
      ddgaddon_dou_engaged_w_sap AS dou_engaged_w_sap,
      ddgaddon_sap AS sap,
      ddgaddon_tagged_sap AS tagged_sap,
      ddgaddon_tagged_follow_on AS tagged_follow_on,
      ddgaddon_ad_click AS ad_click
    FROM
      desktop_data_ddg
  )
UNION ALL
  (
    SELECT
      submission_date,
      'Google' AS partner,
      'mobile' AS device,
      NULL AS channel,
      country,
      dou,
      dou_engaged_w_sap,
      sap,
      tagged_sap,
      tagged_follow_on,
      ad_click
    FROM
      mobile_data_google
  )
UNION ALL
  (
    SELECT
      submission_date,
      'Bing' AS partner,
      'mobile' AS device,
      NULL AS channel,
      country,
      dou,
      dou_engaged_w_sap,
      sap,
      tagged_sap,
      tagged_follow_on,
      ad_click
    FROM
      mobile_data_bing
  )
UNION ALL
  (
    SELECT
      submission_date,
      'DuckDuckGo' AS partner,
      'mobile' AS device,
      NULL AS channel,
      country,
      dou,
      dou_engaged_w_sap,
      sap,
      tagged_sap,
      tagged_follow_on,
      ad_click
    FROM
      mobile_data_ddg
  )
