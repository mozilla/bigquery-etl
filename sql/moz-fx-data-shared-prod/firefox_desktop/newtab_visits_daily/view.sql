CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_desktop.newtab_visits_daily`
AS
SELECT
  'Firefox Desktop' AS app_name,
  (
    is_search_issued
    OR is_content_interaction
    OR is_topsite_interaction
    OR is_widget_interaction
    OR is_wallpaper_interaction
    OR is_other_interaction
  ) AS is_any_interaction,
  (
    is_content_interaction
    OR is_topsite_interaction
    OR is_widget_interaction
    OR is_wallpaper_interaction
    OR is_other_interaction
  ) AS is_nonsearch_interaction,
  (
    is_content_interaction
    AND NOT is_sponsored_content_interaction
  ) AS is_organic_content_interaction,
  (
    is_topsite_interaction
    AND NOT is_sponsored_topsite_interaction
  ) AS is_organic_topsite_interaction,
  *
FROM
  `moz-fx-data-shared-prod.firefox_desktop_derived.newtab_visits_daily_v2`
