SELECT
  submission_date,
  client_id,
  -- mode_last: to pick the most frequent occurring value
  `moz-fx-data-shared-prod.udf.mode_last`(ARRAY_AGG(app_version)) AS app_version,
  `moz-fx-data-shared-prod.udf.mode_last`(ARRAY_AGG(os)) AS os,
  `moz-fx-data-shared-prod.udf.mode_last`(ARRAY_AGG(channel)) AS channel,
  `moz-fx-data-shared-prod.udf.mode_last`(ARRAY_AGG(locale)) AS locale,
  `moz-fx-data-shared-prod.udf.mode_last`(ARRAY_AGG(country)) AS country,
  `moz-fx-data-shared-prod.udf.mode_last`(ARRAY_AGG(homepage_category)) AS homepage_category,
  `moz-fx-data-shared-prod.udf.mode_last`(ARRAY_AGG(newtab_category)) AS newtab_category,
  `moz-fx-data-shared-prod.udf.mode_last`(
    ARRAY_AGG(organic_content_enabled)
  ) AS organic_content_enabled,
  `moz-fx-data-shared-prod.udf.mode_last`(
    ARRAY_AGG(sponsored_content_enabled)
  ) AS sponsored_content_enabled,
  `moz-fx-data-shared-prod.udf.mode_last`(
    ARRAY_AGG(sponsored_topsites_enabled)
  ) AS sponsored_topsites_enabled,
  `moz-fx-data-shared-prod.udf.mode_last`(
    ARRAY_AGG(organic_topsites_enabled)
  ) AS organic_topsites_enabled,
  `moz-fx-data-shared-prod.udf.mode_last`(
    ARRAY_AGG(newtab_search_enabled)
  ) AS newtab_search_enabled,
  COUNT(DISTINCT IF(is_newtab_opened, newtab_visit_id, NULL)) AS all_visits,
  COUNT(DISTINCT IF(is_default_ui, newtab_visit_id, NULL)) AS default_ui_visits,
  COUNT(DISTINCT IF(any_interaction_count > 0, newtab_visit_id, NULL)) AS any_engagement_visits,
  COUNT(
    DISTINCT IF(nonsearch_interaction_count > 0, newtab_visit_id, NULL)
  ) AS nonsearch_engagement_visits,
  COUNT(
    DISTINCT IF(any_content_interaction_count > 0, newtab_visit_id, NULL)
  ) AS any_content_engagement_visits,
  SUM(any_content_click_count) AS any_content_click_count,
  SUM(any_content_impression_count) AS any_content_impression_count,
  COUNT(
    DISTINCT IF(organic_content_interaction_count > 0, newtab_visit_id, NULL)
  ) AS organic_content_engagement_visits,
  SUM(organic_content_click_count) AS organic_content_click_count,
  SUM(organic_content_impression_count) AS organic_content_impression_count,
  COUNT(
    DISTINCT IF(sponsored_content_interaction_count > 0, newtab_visit_id, NULL)
  ) AS sponsored_content_engagement_visits,
  SUM(sponsored_content_click_count) AS sponsored_content_click_count,
  SUM(sponsored_content_impression_count) AS sponsored_content_impression_count,
  COUNT(
    DISTINCT IF(any_topsite_interaction_count > 0, newtab_visit_id, NULL)
  ) AS any_topsite_engagement_visits,
  SUM(any_topsite_click_count) AS any_topsite_click_count,
  SUM(any_topsite_impression_count) AS any_topsite_impression_count,
  COUNT(
    DISTINCT IF(organic_topsite_interaction_count > 0, newtab_visit_id, NULL)
  ) AS organic_topsite_engagement_visits,
  SUM(organic_topsite_click_count) AS organic_topsite_click_count,
  SUM(organic_topsite_impression_count) AS organic_topsite_impression_count,
  COUNT(
    DISTINCT IF(sponsored_topsite_interaction_count > 0, newtab_visit_id, NULL)
  ) AS sponsored_topsite_engagement_visits,
  SUM(sponsored_topsite_click_count) AS sponsored_topsite_click_count,
  SUM(sponsored_topsite_impression_count) AS sponsored_topsite_impression_count,
  COUNT(
    DISTINCT IF(widget_interaction_count > 0, newtab_visit_id, NULL)
  ) AS widget_engagement_visits,
  COUNT(
    DISTINCT IF(other_interaction_count > 0, newtab_visit_id, NULL)
  ) AS others_engagement_visits,
  -- New fields added Aug 2025
  ANY_VALUE(sample_id) AS sample_id,
  ANY_VALUE(profile_group_id) AS profile_group_id,
  `moz-fx-data-shared-prod.udf.mode_last`(ARRAY_AGG(geo_subdivision)) AS geo_subdivision,
  ANY_VALUE(experiments) AS experiments,
  LOGICAL_OR(newtab_weather_enabled) AS newtab_weather_enabled,
  `moz-fx-data-shared-prod.udf.mode_last`(
    ARRAY_AGG(default_search_engine)
  ) AS default_search_engine,
  `moz-fx-data-shared-prod.udf.mode_last`(
    ARRAY_AGG(default_private_search_engine)
  ) AS default_private_search_engine,
  `moz-fx-data-shared-prod.udf.mode_last`(ARRAY_AGG(topsite_rows)) AS topsite_rows,
  `moz-fx-data-shared-prod.udf.mode_last`(
    ARRAY_AGG(topsite_sponsored_tiles_configured)
  ) AS topsite_sponsored_tiles_configured,
  ANY_VALUE(newtab_blocked_sponsors) AS newtab_blocked_sponsors,
  COUNT(
    DISTINCT IF(search_interaction_count > 0, newtab_visit_id, NULL)
  ) AS search_engagement_visits,
  COUNT(DISTINCT IF(search_ad_click_count > 0, newtab_visit_id, NULL)) AS search_ad_click_visits,
  COUNT(
    DISTINCT IF(organic_content_dismissal_count > 0, newtab_visit_id, NULL)
  ) AS organic_content_dismissal_visits,
  COUNT(
    DISTINCT IF(sponsored_content_dismissal_count > 0, newtab_visit_id, NULL)
  ) AS sponsored_content_dismissal_visits,
  COUNT(
    DISTINCT IF(organic_topsite_dismissal_count > 0, newtab_visit_id, NULL)
  ) AS organic_topsite_dismissal_visits,
  COUNT(
    DISTINCT IF(sponsored_topsite_dismissal_count > 0, newtab_visit_id, NULL)
  ) AS sponsored_topsite_dismissal_visits,
  COUNT(
    DISTINCT IF(content_thumbs_up_count > 0, newtab_visit_id, NULL)
  ) AS content_thumbs_up_visits,
  COUNT(
    DISTINCT IF(content_thumbs_down_count > 0, newtab_visit_id, NULL)
  ) AS content_thumbs_down_visits,
  SUM(organic_content_dismissal_count) AS organic_content_dismissal_count,
  SUM(sponsored_content_dismissal_count) AS sponsored_content_dismissal_count,
  SUM(organic_topsite_dismissal_count) AS organic_topsite_dismissal_count,
  SUM(sponsored_topsite_dismissal_count) AS sponsored_topsite_dismissal_count,
  SUM(any_interaction_count) AS any_interaction_count,
  SUM(search_interaction_count) AS search_interaction_count,
  SUM(nonsearch_interaction_count) AS nonsearch_interaction_count,
  SUM(any_content_interaction_count) AS any_content_interaction_count,
  SUM(organic_content_interaction_count) AS organic_content_interaction_count,
  SUM(sponsored_content_interaction_count) AS sponsored_content_interaction_count,
  SUM(any_topsite_interaction_count) AS any_topsite_interaction_count,
  SUM(organic_topsite_interaction_count) AS organic_topsite_interaction_count,
  SUM(sponsored_topsite_interaction_count) AS sponsored_topsite_interaction_count,
  SUM(content_thumbs_up_count) AS content_thumbs_up_count,
  SUM(content_thumbs_down_count) AS content_thumbs_down_count,
  SUM(search_ad_click_count) AS search_ad_click_count,
  SUM(search_ad_impression_count) AS search_ad_impression_count,
  SUM(widget_interaction_count) AS widget_interaction_count,
  SUM(widget_impression_count) AS widget_impression_count,
  SUM(other_interaction_count) AS other_interaction_count,
  SUM(other_impression_count) AS other_impression_count,
  AVG(newtab_visit_duration) AS avg_newtab_visit_duration,
  SUM(newtab_visit_duration) AS cumulative_newtab_visit_duration,
FROM
  `moz-fx-data-shared-prod.firefox_desktop_derived.newtab_visits_daily_v2`
WHERE
  submission_date = @submission_date
GROUP BY
  submission_date,
  client_id
