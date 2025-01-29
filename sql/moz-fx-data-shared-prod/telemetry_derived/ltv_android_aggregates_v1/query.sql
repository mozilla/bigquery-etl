WITH android_metrics AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id,
    LOGICAL_OR(metrics.metrics.boolean.customize_home_contile) AS sponsored_tiles_enabled,
    LOGICAL_OR(metrics.metrics.boolean.metrics_has_top_sites) AS tiles_enabled,
    LOGICAL_OR(metrics.metrics.boolean.customize_home_pocket) AS pocket_enabled,
    LOGICAL_OR(metrics.metrics.boolean.customize_home_sponsored_pocket) AS pocket_spocs_enabled,
    COALESCE(
      SUM(CAST(metrics.metrics.counter.home_screen_home_screen_view_count AS FLOAT64)),
      0
    ) AS homepage_visits
  FROM
    `moz-fx-data-shared-prod.fenix.metrics` AS metrics
  WHERE
    (DATE(metrics.submission_timestamp)) = @submission_date
  GROUP BY
    1,
    2
),
android_events AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id,
    COALESCE(
      COUNTIF(event.category = 'pocket' AND event.name = 'home_recs_shown'),
      0
    ) AS pocket_section_impressions,
    COALESCE(
      COUNTIF(event.category = 'pocket' AND event.name = 'home_recs_spoc_shown'),
      0
    ) AS pocket_spoc_impressions,
    COALESCE(
      COUNTIF(event.category = 'pocket' AND event.name = 'home_recs_spoc_clicked'),
      0
    ) AS pocket_spoc_clicks,
    COALESCE(
      COUNTIF(
        event.category = 'pocket'
        AND event.name IN ('home_recs_story_shown', 'pocket_top_site_clicked')
      ),
      0
    ) AS pocket_recs_clicks,
    COALESCE(
      COUNTIF(event.category = 'top_sites' AND event.name = 'contile_impression'),
      0
    ) AS sponsored_tile_impressions,
    COALESCE(
      COUNTIF(event.category = 'top_sites' AND event.name = 'contile_click'),
      0
    ) AS sponsored_tile_clicks,
    COALESCE(
      COUNTIF(event.category = 'top_sites' AND event.name IN ('open_default', 'open_frecency')),
      0
    ) AS organic_tile_clicks,
  FROM
    `moz-fx-data-shared-prod.fenix.events`
  CROSS JOIN
    UNNEST(events) event
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND event.category IN ('top_sites', 'pocket')
  GROUP BY
    submission_date,
    client_id
),
android_search AS (
  SELECT
    submission_date,
    client_id,
    ANY_VALUE(country) AS country,
    ANY_VALUE(os) AS os,
    ANY_VALUE(channel) AS channel,
    ANY_VALUE(locale) AS locale,
    MAX(mozfun.norm.extract_version(app_version, "major")) AS app_version,
    ANY_VALUE(
      `moz-fx-data-shared-prod`.udf.normalize_search_engine(default_search_engine)
    ) AS default_search_engine,
    DATE_TRUNC(
      ANY_VALUE(DATE_FROM_UNIX_DATE(profile_creation_date)),
      MONTH
    ) AS profile_creation_month,
    SUM(total_uri_count) AS total_uri_count,
    SUM(organic) AS organic,
    SUM(tagged_sap) AS tagged_sap,
    SUM(tagged_follow_on) AS tagged_follow_on,
    SUM(sap) AS sap,
    SUM(ad_click) AS ad_click,
    SUM(ad_click_organic) AS ad_click_organic,
    SUM(search_with_ads) AS search_with_ads,
    SUM(search_with_ads_organic) AS search_with_ads_organic
  FROM
    `moz-fx-data-shared-prod.search.mobile_search_clients_daily`
  WHERE
    submission_date = @submission_date
    AND app_name = "Fenix"
    AND os = "Android"
  GROUP BY
    submission_date,
    client_id
),
android_joined AS (
  SELECT
    s.*,
    sponsored_tiles_enabled,
    tiles_enabled,
    pocket_enabled,
    pocket_spocs_enabled,
    ad_click + COALESCE(sponsored_tile_clicks) > 0 AS client_with_ad_engagement,
    ad_click > 0 AS client_with_search_ad_engagement,
    COALESCE(sponsored_tile_clicks) > 0 AS client_with_homepage_ad_engagement,
    COALESCE(pocket_section_impressions, 0) AS pocket_section_impressions,
    COALESCE(pocket_spoc_impressions, 0) AS pocket_spoc_impressions,
    COALESCE(pocket_spoc_clicks, 0) AS pocket_spoc_clicks,
    COALESCE(pocket_recs_clicks, 0) AS pocket_recs_clicks,
    COALESCE(sponsored_tile_impressions, 0) AS sponsored_tile_impressions,
    COALESCE(sponsored_tile_clicks, 0) AS sponsored_tile_clicks,
    COALESCE(organic_tile_clicks, 0) AS organic_tile_clicks,
    COALESCE(homepage_visits, 0) AS homepage_visits,
  FROM
    android_search s
  LEFT JOIN
    android_metrics
    USING (submission_date, client_id)
  LEFT JOIN
    android_events
    USING (submission_date, client_id)
)
SELECT
  submission_date,
  country,
  os,
  channel,
  locale,
  app_version,
  default_search_engine,
  profile_creation_month,
  sponsored_tiles_enabled,
  tiles_enabled,
  pocket_enabled,
  pocket_spocs_enabled,
  COUNT(client_id) AS client_count,
  COUNTIF(client_with_ad_engagement) AS clients_with_ad_engagement,
  COUNTIF(client_with_homepage_ad_engagement) AS clients_with_homepage_ad_engagement,
  COUNTIF(client_with_search_ad_engagement) AS clients_with_search_ad_engagement,
  AVG(total_uri_count) AS total_uri_count_avg,
  STDDEV(total_uri_count) AS total_uri_count_stddev,
  AVG(organic) AS organic_avg,
  STDDEV(organic) AS organic_stddev,
  AVG(tagged_sap) AS tagged_sap_avg,
  STDDEV(tagged_sap) AS tagged_sap_stddev,
  AVG(tagged_follow_on) AS tagged_follow_on_avg,
  STDDEV(tagged_follow_on) AS tagged_follow_on_stddev,
  AVG(sap) AS sap_avg,
  STDDEV(sap) AS sap_stddev,
  AVG(ad_click) AS ad_click_avg,
  STDDEV(ad_click) AS ad_click_stddev,
  AVG(ad_click_organic) AS ad_click_organic_avg,
  STDDEV(ad_click_organic) AS ad_click_organic_stddev,
  AVG(search_with_ads) AS search_with_ads_avg,
  STDDEV(search_with_ads) AS search_with_ads_stddev,
  AVG(search_with_ads_organic) AS search_with_ads_organic_avg,
  STDDEV(search_with_ads_organic) AS search_with_ads_organic_stddev,
  AVG(homepage_visits) AS homepage_visits_avg,
  STDDEV(homepage_visits) AS homepage_visits_stddev,
  AVG(pocket_section_impressions) AS pocket_section_impressions_avg,
  STDDEV(pocket_section_impressions) AS pocket_section_impressions_stddev,
  AVG(pocket_spoc_impressions) AS pocket_spoc_impressions_avg,
  STDDEV(pocket_spoc_impressions) AS pocket_spoc_impressions_stddev,
  AVG(pocket_spoc_clicks) AS pocket_spoc_clicks_avg,
  STDDEV(pocket_spoc_clicks) AS pocket_spoc_clicks_stddev,
  AVG(pocket_recs_clicks) AS pocket_recs_clicks_avg,
  STDDEV(pocket_recs_clicks) AS pocket_recs_clicks_stddev,
  AVG(sponsored_tile_impressions) AS sponsored_tile_impressions_avg,
  STDDEV(sponsored_tile_impressions) AS sponsored_tile_impressions_stddev,
  AVG(sponsored_tile_clicks) AS sponsored_tile_clicks_avg,
  STDDEV(sponsored_tile_clicks) AS sponsored_tile_clicks_stddev,
  AVG(organic_tile_clicks) AS organic_tile_clicks_avg,
  STDDEV(organic_tile_clicks) AS organic_tile_clicks_stddev
FROM
  android_joined
GROUP BY
  submission_date,
  country,
  os,
  channel,
  locale,
  app_version,
  default_search_engine,
  profile_creation_month,
  sponsored_tiles_enabled,
  tiles_enabled,
  pocket_enabled,
  pocket_spocs_enabled
