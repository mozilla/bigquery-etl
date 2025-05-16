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
  COUNT(
    DISTINCT IF(
      (
        is_search_issued
        OR is_content_interaction
        OR is_topsite_interaction
        OR is_widget_interaction
        OR is_wallpaper_interaction
        OR is_other_interaction
      ),
      newtab_visit_id,
      NULL
    )
  ) AS any_engagement_visits,
  COUNT(
    DISTINCT IF(
      (
        is_content_interaction
        OR is_topsite_interaction
        OR is_widget_interaction
        OR is_wallpaper_interaction
        OR is_other_interaction
      ),
      newtab_visit_id,
      NULL
    )
  ) AS nonsearch_engagement_visits,
  COUNT(
    DISTINCT IF(is_content_interaction OR is_sponsored_content_interaction, newtab_visit_id, NULL)
  ) AS any_content_engagement_visits,
  SUM(any_content_click_count) AS any_content_click_count,
  SUM(any_content_impression_count) AS any_content_impression_count,
  COUNT(
    DISTINCT IF(
      (is_content_interaction AND NOT is_sponsored_content_interaction),
      newtab_visit_id,
      NULL
    )
  ) AS organic_content_engagement_visits,
  SUM(organic_content_click_count) AS organic_content_click_count,
  SUM(organic_content_impression_count) AS organic_content_impression_count,
  COUNT(
    DISTINCT IF(is_sponsored_content_interaction, newtab_visit_id, NULL)
  ) AS sponsored_content_engagement_visits,
  SUM(sponsored_content_click_count) AS sponsored_content_click_count,
  SUM(sponsored_content_impression_count) AS sponsored_content_impression_count,
  COUNT(
    DISTINCT IF(is_topsite_interaction, newtab_visit_id, NULL)
  ) AS any_topsite_engagement_visits,
  SUM(any_topsite_click_count) AS any_topsite_click_count,
  SUM(any_topsite_impression_count) AS any_topsite_impression_count,
  COUNT(
    DISTINCT IF(
      (is_topsite_interaction AND NOT is_sponsored_topsite_interaction),
      newtab_visit_id,
      NULL
    )
  ) AS organic_topsite_engagement_visits,
  SUM(organic_topsite_click_count) AS organic_topsite_click_count,
  SUM(organic_topsite_impression_count) AS organic_topsite_impression_count,
  COUNT(
    DISTINCT IF(is_sponsored_topsite_interaction, newtab_visit_id, NULL)
  ) AS sponsored_topsite_engagement_visits,
  SUM(sponsored_topsite_click_count) AS sponsored_topsite_click_count,
  SUM(sponsored_topsite_impression_count) AS sponsored_topsite_impression_count,
  COUNT(DISTINCT IF(is_widget_interaction, newtab_visit_id, NULL)) AS widget_engagement_visits,
  COUNT(DISTINCT IF(is_other_interaction, newtab_visit_id, NULL)) AS others_engagement_visits,
FROM
  `moz-fx-data-shared-prod.firefox_desktop_derived.newtab_visits_daily_v2`
WHERE
  submission_date = @submission_date
GROUP BY
  submission_date,
  client_id
