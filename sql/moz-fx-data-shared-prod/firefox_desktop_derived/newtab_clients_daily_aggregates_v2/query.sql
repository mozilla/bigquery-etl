SELECT
  submission_date,
  app_version,
  os,
  channel,
  locale,
  browser_version,
  country,
  newtab_homepage_category,
  newtab_newtab_category,
  organic_content_enabled,
  sponsored_content_enabled,
  sponsored_topsites_enabled,
  organic_topsites_enabled,
  newtab_search_enabled,
  SUM(all_visits) AS all_visits,
  COUNT(DISTINCT client_id) AS newtab_clients,
  SUM(default_ui_visits) AS default_ui_visits,
  COUNT(DISTINCT IF(default_ui_visits > 0, client_id, NULL)) AS default_ui_clients,
  SUM(any_engagement_visits) AS any_engagement_visits,
  COUNT(DISTINCT IF(any_engagement_visits > 0, client_id, NULL)) AS any_engagement_clients,
  SUM(nonsearch_engagement_visits) AS nonsearch_engagement_visits,
  COUNT(
    DISTINCT IF(nonsearch_engagement_visits > 0, client_id, NULL)
  ) AS nonsearch_engagement_clients,
  SUM(any_content_engagement_visits) AS any_content_engagement_visits,
  COUNT(
    DISTINCT IF(any_content_engagement_visits > 0, client_id, NULL)
  ) AS any_content_engagement_clients,
  SUM(any_content_click_count) AS any_content_click_count,
  SUM(any_content_impression_count) AS any_content_impression_count,
  SUM(organic_content_engagement_visits) AS organic_content_engagement_visits,
  COUNT(
    DISTINCT IF(organic_content_engagement_visits > 0, client_id, NULL)
  ) AS organic_content_engagement_clients,
  SUM(organic_content_click_count) AS organic_content_click_count,
  SUM(organic_content_impression_count) AS organic_content_impression_count,
  SUM(sponsored_content_engagement_visits) AS sponsored_content_engagement_visits,
  COUNT(
    DISTINCT IF(sponsored_content_engagement_visits > 0, client_id, NULL)
  ) AS sponsored_content_engagement_clients,
  SUM(sponsored_content_click_count) AS sponsored_content_click_count,
  SUM(sponsored_content_impression_count) AS sponsored_content_impression_count,
  SUM(any_topsites_engagement_visits) AS any_topsites_engagement_visits,
  COUNT(
    DISTINCT IF(any_topsites_engagement_visits > 0, client_id, NULL)
  ) AS any_topsites_engagement_clients,
  SUM(any_topsites_click_count) AS any_topsites_click_count,
  SUM(any_topsites_impression_count) AS any_topsites_impression_count,
  SUM(organic_topsite_engagement_visits) AS organic_topsite_engagement_visits,
  COUNT(
    DISTINCT IF(organic_topsite_engagement_visits > 0, client_id, NULL)
  ) AS organic_topsite_engagement_clients,
  SUM(organic_topsites_click_count) AS organic_topsites_click_count,
  SUM(organic_topsites_impression_count) AS organic_topsites_impression_count,
  SUM(sponsored_topsite_engagement_visits) AS sponsored_topsite_engagement_visits,
  COUNT(
    DISTINCT IF(sponsored_topsite_engagement_visits > 0, client_id, NULL)
  ) AS sponsored_topsite_engagement_clients,
  SUM(sponsored_topsites_click_count) AS sponsored_topsites_click_count,
  SUM(sponsored_topsites_impression_count) AS sponsored_topsites_impression_count,
  SUM(widget_engagement_visits) AS widget_engagement_visits,
  COUNT(DISTINCT IF(widget_engagement_visits > 0, client_id, NULL)) AS widget_engagement_clients,
  SUM(others_engagement_visits) AS others_engagement_visits,
  COUNT(DISTINCT IF(others_engagement_visits > 0, client_id, NULL)) AS others_engagement_clients,
FROM
  `moz-fx-data-shared-prod.firefox_desktop_derived.newtab_clients_daily_v2`
WHERE
  submission_date = @submission_date
GROUP BY
  submission_date,
  app_version,
  os,
  channel,
  locale,
  browser_version,
  country,
  newtab_homepage_category,
  newtab_newtab_category,
  organic_content_enabled,
  sponsored_content_enabled,
  sponsored_topsites_enabled,
  organic_topsites_enabled,
  newtab_search_enabled
