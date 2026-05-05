WITH ads_metadata AS (
  SELECT
    ad_id,
    sponsor AS advertiser,
    flight_id,
    flight_name,
    IF(
      ARRAY_LENGTH(SPLIT(flight_name, '[')) > 1
      AND ARRAY_LENGTH(SPLIT(SPLIT(flight_name, '[')[SAFE_OFFSET(1)], ']')) > 0,
      SPLIT(SPLIT(flight_name, '[')[SAFE_OFFSET(1)], ']')[SAFE_OFFSET(0)],
      'US'
    ) AS country,
    campaign_id,
    TRIM(creative_title) AS title,
    TRIM(creative_url) AS ad_url,
    image_url,
    rate_type,
    price,
    REGEXP_EXTRACT(creative_url, '[?&]pid=([^&]*)') AS pid,
    REGEXP_EXTRACT(creative_url, '[?&]external_param=([^&]*)') AS external_param,
    campaign_name,
    STRING_AGG(DISTINCT sites.site_name, ', ') AS site_name,
    STRING_AGG(DISTINCT zones.zone_name, ', ') AS zone_name
  FROM
    `moz-fx-data-shared-prod.ads_derived.kevel_metadata_v2`,
    UNNEST(sites) AS sites,
    UNNEST(zones) AS zones
  WHERE
    sites.site_name = 'Firefox Production'
  GROUP BY
    ad_id,
    advertiser,
    flight_id,
    flight_name,
    country,
    campaign_id,
    title,
    ad_url,
    image_url,
    rate_type,
    price,
    pid,
    external_param,
    campaign_name
),
newtab_engagement AS (
  SELECT
    submission_date,
    recommendation_id,
    tile_id AS spoc_id,
    position,
    impression_count,
    click_count,
    save_count,
    dismiss_count
  FROM
    `moz-fx-data-shared-prod.telemetry_derived.newtab_interactions_hourly_v1`
  WHERE
    submission_date = @submission_date
    -- we only have form_factor & placement from UAPI - these fields are NULL for legacy and glean telemetry
    AND (form_factor IS NULL OR form_factor = 'desktop')
    AND (
      placement IS NULL
      OR placement IN (
        'newtab_spocs',
        'newtab_rectangle',
        'newtab_billboard',
        'newtab_leaderboard',
        'newtab_stories_1',
        'newtab_stories_2',
        'newtab_stories_3',
        'newtab_stories_4',
        'newtab_stories_5',
        'newtab_stories_6'
      )
    )
)
SELECT
  n.submission_date,
  n.spoc_id,
  n.position,
  a.advertiser,
  a.campaign_name,
  a.campaign_id,
  a.title,
  a.flight_id,
  a.flight_name AS creative_type,
  a.site_name,
  a.zone_name,
  a.country,
  a.rate_type,
  a.pid,
  a.ad_url,
  a.image_url,
  a.external_param,
  SUM(n.impression_count) AS impressions,
  SUM(n.click_count) AS clicks,
  COALESCE(SUM(n.click_count) / NULLIF(SUM(n.impression_count), 0), 0) AS click_rate,
  SUM(n.dismiss_count) AS dismisses,
  COALESCE(SUM(n.dismiss_count) / NULLIF(SUM(n.impression_count), 0), 0) AS dismiss_rate,
  SUM(n.save_count) AS saves,
  COALESCE(SUM(n.save_count) / NULLIF(SUM(n.impression_count), 0), 0) AS save_rate,
  SUM(
    CASE
      WHEN a.rate_type = 'CPC'
        THEN n.click_count * a.price
      WHEN a.rate_type = 'CPM'
        THEN n.impression_count / 1000 * a.price
      ELSE 0
    END
  ) AS spend
FROM
  newtab_engagement n
INNER JOIN
  ads_metadata a
  ON a.ad_id = n.spoc_id
GROUP BY
  submission_date,
  spoc_id,
  position,
  advertiser,
  campaign_name,
  campaign_id,
  title,
  flight_id,
  creative_type,
  site_name,
  zone_name,
  country,
  rate_type,
  pid,
  ad_url,
  image_url,
  external_param
