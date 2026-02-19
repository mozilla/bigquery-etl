WITH acer_cohort AS (
  SELECT
    client_id
  FROM
    `moz-fx-data-shared-prod.search.acer_cohort`
)
SELECT
  scd.submission_date,
  scd.addon_version,
  scd.app_version,
  scd.country,
  scd.distribution_id,
  scd.engine,
  scd.locale,
  scd.search_cohort,
  scd.source,
  scd.default_search_engine,
  scd.default_private_search_engine,
  scd.os,
  scd.os_version,
  scd.is_default_browser,
  scd.policies_is_enterprise,
  scd.channel,
  `moz-fx-data-shared-prod`.udf.normalize_search_engine(scd.engine) AS normalized_engine,
  CASE
    WHEN default_search_engine LIKE '%google%'
      THEN "Google"
    WHEN default_search_engine LIKE '%bing%'
      THEN "Bing"
    WHEN default_search_engine LIKE '%ddg%'
      OR default_search_engine LIKE '%duckduckgo%'
      THEN "DuckDuckGo"
    ELSE NULL
  END AS normalized_default_search_engine,
  scd.is_sap_monetizable,
  COUNT(*) AS client_count,
  SUM(scd.organic) AS organic,
  SUM(scd.tagged_sap) AS tagged_sap,
  SUM(scd.tagged_follow_on) AS tagged_follow_on,
  SUM(scd.sap) AS sap,
  SUM(scd.ad_click) AS ad_click,
  SUM(scd.ad_click_organic) AS ad_click_organic,
  SUM(scd.search_with_ads) AS search_with_ads,
  SUM(scd.search_with_ads_organic) AS search_with_ads_organic,
  SUM(scd.unknown) AS unknown,
  CASE
    WHEN distribution_id IS NULL
      OR (distribution_id NOT LIKE '%acer%' AND ac.client_id IS NULL)
      THEN TRUE
    ELSE FALSE
  END AS is_acer_cohort
FROM
  `moz-fx-data-shared-prod.search_derived.search_clients_daily_v8` scd
LEFT JOIN
  acer_cohort ac
  ON scd.client_id = ac.client_id
WHERE
  scd.submission_date = @submission_date
GROUP BY
  scd.submission_date,
  scd.addon_version,
  scd.app_version,
  scd.country,
  scd.distribution_id,
  scd.engine,
  scd.locale,
  scd.search_cohort,
  scd.source,
  scd.default_search_engine,
  scd.default_private_search_engine,
  scd.os,
  scd.os_version,
  scd.is_default_browser,
  scd.policies_is_enterprise,
  scd.channel,
  normalized_engine,
  scd.is_sap_monetizable,
  is_acer_cohort
