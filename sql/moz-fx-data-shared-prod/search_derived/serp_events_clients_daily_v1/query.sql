SELECT
  submission_date,
  glean_client_id,
  legacy_telemetry_client_id,
  profile_group_id,
  `moz-fx-data-shared-prod`.udf.normalize_search_engine(search_engine) AS partner,
  'desktop' AS device,
  normalized_country_code,
  normalized_channel,
  os,
  browser_version_info.major_version AS browser_major_version,
  browser_version_info.minor_version AS browser_minor_version,
  ANY_VALUE(experiments) AS experiments,
  LOGICAL_OR(ad_blocker_inferred) AS ad_blocker_inferred,
  COUNT(
    DISTINCT IF(
      REGEXP_CONTAINS(sap_source, 'urlbar')
      OR sap_source IN ('searchbar', 'contextmenu', 'webextension', 'system'),
      impression_id,
      NULL
    )
  ) AS sap,
  COUNTIF(is_tagged) AS tagged_sap,
  COUNTIF(is_tagged AND REGEXP_CONTAINS(sap_source, 'follow_on')) AS tagged_follow_on,
  SUM(num_ad_clicks) AS ad_click,
  COUNTIF(num_ads_visible > 0) AS search_with_ads,
  COUNTIF(NOT is_tagged) AS organic,
  SUM(IF(NOT is_tagged, num_ad_clicks, 0)) AS ad_click_organic,
  COUNTIF(num_ads_visible > 0 AND NOT is_tagged) AS search_with_ads_organic,
    -- serp_events does not have distribution ID or partner codes to calculate monetizable SAP
  COUNTIF(ad_blocker_inferred) AS sap_with_ad_blocker_inferred,
  SUM(num_ads_visible) AS num_ads_visible,
  SUM(num_ads_blocked) AS num_ads_blocked,
  SUM(num_ads_notshowing) AS num_ads_notshowing,
  COUNTIF(abandon_reason IS NOT NULL) AS num_abandoned_serp
FROM
  `moz-fx-data-shared-prod.firefox_desktop.serp_events`
WHERE
  submission_date = DATE_SUB(@submission_date, INTERVAL 1 DAY)
GROUP BY
  submission_date,
  glean_client_id,
  legacy_telemetry_client_id,
  profile_group_id,
  partner,
  device,
  normalized_country_code,
  normalized_channel,
  os,
  browser_major_version,
  browser_minor_version
