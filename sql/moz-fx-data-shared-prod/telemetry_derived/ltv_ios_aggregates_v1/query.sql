WITH ios_metrics AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id,
    ANY_VALUE(metrics.metrics.boolean.top_sites_sponsored_shortcuts) AS sponsored_tiles_enabled,
    LOGICAL_OR(metrics.metrics.boolean.firefox_home_page_pocket_stories_visible) AS pocket_enabled,
    SUM(
      COALESCE(metrics.metrics.counter.pocket_section_impressions, 0)
    ) AS pocket_section_impressions,
    SUM(COALESCE(metrics.metrics.counter.pocket_open_story, 0)) AS pocket_story_clicks,
    SUM(
      COALESCE(
        (
          SELECT
            SUM(hv.value)
          FROM
            UNNEST(metrics.metrics.labeled_counter.firefox_home_page_firefox_homepage_origin) AS hv
        ),
        0
      )
    ) AS homepage_visits
  FROM
    `mozdata.firefox_ios.metrics` AS metrics
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    client_id
),
ios_events AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id,
    COALESCE(
      COUNTIF(event.category = "top_sites" AND event.name = "contile_click"),
      0
    ) AS sponsored_tile_clicks,
    COALESCE(
      COUNTIF(event.category = "top_sites" AND event.name = "contile_impression"),
      0
    ) AS sponsored_tile_impressions,
    COALESCE(
      COUNTIF(event.category = "top_sites" AND event.name = "tile_pressed"),
      0
    ) AS organic_tile_clicks,
  FROM
    `moz-fx-data-shared-prod.firefox_ios.events`
  CROSS JOIN
    UNNEST(events) event
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    client_id
),
ios_search AS (
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
    AND app_name = "Fennec"
    AND os = "iOS"
  GROUP BY
    submission_date,
    client_id
),
ios_joined AS (
  SELECT
    s.*,
    ad_click + COALESCE(sponsored_tile_clicks) > 0 AS client_with_ad_engagement,
    ad_click > 0 AS client_with_search_ad_engagement,
    COALESCE(sponsored_tile_clicks) > 0 AS client_with_homepage_ad_engagement,
    sponsored_tiles_enabled,
    pocket_enabled,
    COALESCE(homepage_visits, 0) AS homepage_visits,
    COALESCE(pocket_story_clicks, 0) AS pocket_story_clicks,
    COALESCE(pocket_section_impressions, 0) AS pocket_section_impressions,
    COALESCE(sponsored_tile_clicks, 0) AS sponsored_tile_clicks,
    COALESCE(sponsored_tile_impressions, 0) AS sponsored_tile_impressions,
    COALESCE(organic_tile_clicks, 0) AS organic_tile_clicks,
  FROM
    ios_search s
  LEFT JOIN
    ios_metrics
    USING (client_id, submission_date)
  LEFT JOIN
    ios_events
    USING (client_id, submission_date)
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
  pocket_enabled,
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
  AVG(pocket_story_clicks) AS pocket_story_clicks_avg,
  STDDEV(pocket_story_clicks) AS pocket_story_clicks_stddev,
  AVG(pocket_section_impressions) AS pocket_section_impressions_avg,
  STDDEV(pocket_section_impressions) AS pocket_section_impressions_stddev,
  AVG(sponsored_tile_clicks) AS sponsored_tile_clicks_avg,
  STDDEV(sponsored_tile_clicks) AS sponsored_tile_clicks_stddev,
  AVG(sponsored_tile_impressions) AS sponsored_tile_impressions_avg,
  STDDEV(sponsored_tile_impressions) AS sponsored_tile_impressions_stddev,
  AVG(organic_tile_clicks) AS organic_tile_clicks_avg,
  STDDEV(organic_tile_clicks) AS organic_tile_clicks_stddev,
FROM
  ios_joined
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
  pocket_enabled
