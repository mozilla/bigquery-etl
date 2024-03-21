WITH visits_data AS (
  SELECT
    client_id,
    submission_date,
    ANY_VALUE(legacy_telemetry_client_id) AS legacy_telemetry_client_id,
    COUNT(DISTINCT newtab_visit_id) AS newtab_visit_count,
    ANY_VALUE(normalized_os) AS normalized_os,
    ANY_VALUE(normalized_os_version) AS normalized_os_version,
    ANY_VALUE(country_code) AS country_code,
    ANY_VALUE(locale) AS locale,
    ANY_VALUE(channel) AS channel,
    ANY_VALUE(browser_version) AS browser_version,
    ANY_VALUE(browser_name) AS browser_name,
    ANY_VALUE(default_search_engine) AS default_search_engine,
    ANY_VALUE(default_private_search_engine) AS default_private_search_engine,
    MAX(pocket_is_signed_in) AS pocket_is_signed_in,
    MIN(pocket_enabled) AS pocket_enabled,
    MIN(pocket_sponsored_stories_enabled) AS pocket_sponsored_stories_enabled,
    MIN(topsites_enabled) AS topsites_enabled,
    ANY_VALUE(newtab_homepage_category) AS newtab_homepage_category,
    ANY_VALUE(newtab_newtab_category) AS newtab_newtab_category,
    ANY_VALUE(topsites_rows) AS topsites_rows,
    ANY_VALUE(experiments) AS experiments,
    SUM(CASE WHEN had_non_impression_engagement THEN 1 ELSE 0 END) AS had_non_impression_engagement,
    SUM(CASE WHEN had_non_search_engagement THEN 1 ELSE 0 END) AS had_non_search_engagement,
    MAX(is_new_profile) AS is_new_profile,
    ANY_VALUE(activity_segment) AS activity_segment
  FROM
    `moz-fx-data-shared-prod.telemetry_derived.newtab_visits_v1`
  WHERE
    submission_date = @submission_date
  GROUP BY
    client_id,
    submission_date
),
search_data AS (
  SELECT
    client_id,
    SUM(searches, 0) AS searches,
    SUM(tagged_search_ad_clicks, 0) AS tagged_search_ad_clicks,
    SUM(tagged_search_ad_impressions, 0) AS tagged_search_ad_impressions,
    SUM(follow_on_search_ad_clicks, 0) AS follow_on_search_ad_clicks,
    SUM(follow_on_search_ad_impressions, 0) AS follow_on_search_ad_impressions,
    SUM(tagged_follow_on_search_ad_clicks, 0) AS tagged_follow_on_search_ad_clicks,
    SUM(tagged_follow_on_search_ad_impressions, 0) AS tagged_follow_on_search_ad_impressions,
  FROM
    `moz-fx-data-shared-prod.telemetry_derived.newtab_visits_v1`
  CROSS JOIN
    UNNEST(search_interactions)
  WHERE
    submission_date = @submission_date
  GROUP BY
    client_id
),
tiles_data AS (
  SELECT
    client_id,
    SUM(topsite_tile_clicks, 0) AS topsite_tile_clicks,
    SUM(sponsored_topsite_tile_clicks, 0) AS sponsored_topsite_tile_clicks,
    SUM(organic_topsite_tile_clicks, 0) AS organic_topsite_tile_clicks,
    SUM(topsite_tile_impressions, 0) AS topsite_tile_impressions,
    SUM(sponsored_topsite_tile_impressions, 0) AS sponsored_topsite_tile_impressions,
    SUM(organic_topsite_tile_impressions, 0) AS organic_topsite_tile_impressions,
    SUM(topsite_tile_dismissals, 0) AS topsite_tile_dismissals,
    SUM(sponsored_topsite_tile_dismissals, 0) AS sponsored_topsite_tile_dismissals,
    SUM(organic_topsite_tile_dismissals, 0) AS organic_topsite_tile_dismissals,
  FROM
    `moz-fx-data-shared-prod.telemetry_derived.newtab_visits_v1`
  CROSS JOIN
    UNNEST(topsite_tile_interactions)
  WHERE
    submission_date = @submission_date
  GROUP BY
    client_id
),
pocket_data AS (
  SELECT
    client_id,
    SUM(pocket_impressions, 0) AS pocket_impressions,
    SUM(sponsored_pocket_impressions, 0) AS sponsored_pocket_impressions,
    SUM(organic_pocket_impressions, 0) AS organic_pocket_impressions,
    SUM(pocket_clicks, 0) AS pocket_clicks,
    SUM(sponsored_pocket_clicks, 0) AS sponsored_pocket_clicks,
    SUM(organic_pocket_clicks, 0) AS organic_pocket_clicks,
    SUM(pocket_saves, 0) AS pocket_saves,
    SUM(sponsored_pocket_saves, 0) AS sponsored_pocket_saves,
    SUM(organic_pocket_saves, 0) AS organic_pocket_saves,
  FROM
    `moz-fx-data-shared-prod.telemetry_derived.newtab_visits_v1`
  CROSS JOIN
    UNNEST(pocket_interactions)
  WHERE
    submission_date = @submission_date
  GROUP BY
    client_id
)
SELECT
  visits_data.*,
  -- COALESCE calls for visits where no interactions with a surface were performed and are all Null
  COALESCE(searches, 0) AS searches,
  COALESCE(tagged_search_ad_clicks, 0) AS tagged_search_ad_clicks,
  COALESCE(tagged_search_ad_impressions, 0) AS tagged_search_ad_impressions,
  COALESCE(follow_on_search_ad_clicks, 0) AS follow_on_search_ad_clicks,
  COALESCE(follow_on_search_ad_impressions, 0) AS follow_on_search_ad_impressions,
  COALESCE(tagged_follow_on_search_ad_clicks, 0) AS tagged_follow_on_search_ad_clicks,
  COALESCE(tagged_follow_on_search_ad_impressions, 0) AS tagged_follow_on_search_ad_impressions,
  COALESCE(topsite_tile_clicks, 0) AS topsite_tile_clicks,
  COALESCE(sponsored_topsite_tile_clicks, 0) AS sponsored_topsite_tile_clicks,
  COALESCE(organic_topsite_tile_clicks, 0) AS organic_topsite_tile_clicks,
  COALESCE(topsite_tile_impressions, 0) AS topsite_tile_impressions,
  COALESCE(sponsored_topsite_tile_impressions, 0) AS sponsored_topsite_tile_impressions,
  COALESCE(organic_topsite_tile_impressions, 0) AS organic_topsite_tile_impressions,
  COALESCE(topsite_tile_dismissals, 0) AS topsite_tile_dismissals,
  COALESCE(sponsored_topsite_tile_dismissals, 0) AS sponsored_topsite_tile_dismissals,
  COALESCE(organic_topsite_tile_dismissals, 0) AS organic_topsite_tile_dismissals,
  COALESCE(pocket_impressions, 0) AS pocket_impressions,
  COALESCE(sponsored_pocket_impressions, 0) AS sponsored_pocket_impressions,
  COALESCE(organic_pocket_impressions, 0) AS organic_pocket_impressions,
  COALESCE(pocket_clicks, 0) AS pocket_clicks,
  COALESCE(sponsored_pocket_clicks, 0) AS sponsored_pocket_clicks,
  COALESCE(organic_pocket_clicks, 0) AS organic_pocket_clicks,
  COALESCE(pocket_saves, 0) AS pocket_saves,
  COALESCE(sponsored_pocket_saves, 0) AS sponsored_pocket_saves,
  COALESCE(organic_pocket_saves, 0) AS organic_pocket_saves,
FROM
  visits_data
LEFT JOIN
  search_data
  USING (client_id)
LEFT JOIN
  tiles_data
  USING (client_id)
LEFT JOIN
  pocket_data
  USING (client_id)
