SELECT
  submission_date,
  app_version,
  os,
  channel,
  locale,
  country,
  homepage_category,
  newtab_category,
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
  SUM(any_topsite_engagement_visits) AS any_topsite_engagement_visits,
  COUNT(
    DISTINCT IF(any_topsite_engagement_visits > 0, client_id, NULL)
  ) AS any_topsite_engagement_clients,
  SUM(any_topsite_click_count) AS any_topsite_click_count,
  SUM(any_topsite_impression_count) AS any_topsite_impression_count,
  SUM(organic_topsite_engagement_visits) AS organic_topsite_engagement_visits,
  COUNT(
    DISTINCT IF(organic_topsite_engagement_visits > 0, client_id, NULL)
  ) AS organic_topsite_engagement_clients,
  SUM(organic_topsite_click_count) AS organic_topsite_click_count,
  SUM(organic_topsite_impression_count) AS organic_topsite_impression_count,
  SUM(sponsored_topsite_engagement_visits) AS sponsored_topsite_engagement_visits,
  COUNT(
    DISTINCT IF(sponsored_topsite_engagement_visits > 0, client_id, NULL)
  ) AS sponsored_topsite_engagement_clients,
  SUM(sponsored_topsite_click_count) AS sponsored_topsite_click_count,
  SUM(sponsored_topsite_impression_count) AS sponsored_topsite_impression_count,
  SUM(widget_engagement_visits) AS widget_engagement_visits,
  COUNT(DISTINCT IF(widget_engagement_visits > 0, client_id, NULL)) AS widget_engagement_clients,
  SUM(others_engagement_visits) AS others_engagement_visits,
  COUNT(DISTINCT IF(others_engagement_visits > 0, client_id, NULL)) AS others_engagement_clients,
  -- New fields added Sep 2025
  COUNT(
    DISTINCT IF(organic_content_dismissal_visits > 0, client_id, NULL)
  ) AS organic_content_dismissal_clients,
  SUM(organic_content_dismissal_visits) AS organic_content_dismissal_visits,
  SUM(organic_content_dismissal_count) AS organic_content_dismissal_count,
  COUNT(
    DISTINCT IF(sponsored_content_dismissal_visits > 0, client_id, NULL)
  ) AS sponsored_content_dismissal_clients,
  SUM(sponsored_content_dismissal_visits) AS sponsored_content_dismissal_visits,
  SUM(sponsored_content_dismissal_count) AS sponsored_content_dismissal_count,
  COUNT(
    DISTINCT IF(organic_topsite_dismissal_visits > 0, client_id, NULL)
  ) AS organic_topsite_dismissal_clients,
  SUM(organic_topsite_dismissal_visits) AS organic_topsite_dismissal_visits,
  SUM(organic_topsite_dismissal_count) AS organic_topsite_dismissal_count,
  COUNT(
    DISTINCT IF(sponsored_topsite_dismissal_visits > 0, client_id, NULL)
  ) AS sponsored_topsite_dismissal_clients,
  SUM(sponsored_topsite_dismissal_visits) AS sponsored_topsite_dismissal_visits,
  SUM(sponsored_topsite_dismissal_count) AS sponsored_topsite_dismissal_count,
  COUNT(DISTINCT IF(content_thumbs_up_visits > 0, client_id, NULL)) AS content_thumbs_up_clients,
  SUM(content_thumbs_up_visits) AS content_thumbs_up_visits,
  SUM(content_thumbs_up_count) AS content_thumbs_up_count,
  COUNT(
    DISTINCT IF(content_thumbs_down_visits > 0, client_id, NULL)
  ) AS content_thumbs_down_clients,
  SUM(content_thumbs_down_visits) AS content_thumbs_down_visits,
  SUM(content_thumbs_down_count) AS content_thumbs_down_count,
  COUNT(DISTINCT IF(search_ad_click_visits > 0, client_id, NULL)) AS search_ad_click_clients,
  SUM(search_ad_click_visits) AS search_ad_click_visits,
  SUM(search_ad_click_count) AS search_ad_click_count,
  SUM(search_ad_impression_count) AS search_ad_impression_count,
  COUNT(DISTINCT IF(search_engagement_visits > 0, client_id, NULL)) AS search_engagement_clients,
  SUM(search_engagement_visits) AS search_engagement_visits,
  SUM(search_interaction_count) AS search_interaction_count,
  SUM(widget_interaction_count) AS widget_interaction_count,
  SUM(widget_impression_count) AS widget_impression_count,
  SUM(others_engagement_visits) AS other_engagement_visits,
  SUM(other_interaction_count) AS other_interaction_count,
  SUM(other_impression_count) AS other_impression_count,
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
  country,
  homepage_category,
  newtab_category,
  organic_content_enabled,
  sponsored_content_enabled,
  sponsored_topsites_enabled,
  organic_topsites_enabled,
  newtab_search_enabled
