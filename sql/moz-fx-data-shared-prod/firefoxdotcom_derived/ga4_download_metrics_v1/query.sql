-- Daily rollup of product_download events on firefox.com.
WITH downloads AS (
  SELECT
    user_pseudo_id AS ga_client_id,
    CAST(mozfun.map.get_key(event_params, 'ga_session_id').int_value AS STRING) AS ga_session_id,
    mozfun.map.get_key(event_params, 'page_location').string_value AS page_location,
    geo.country AS country,
    -- Blank strings become NULL.
    NULLIF(mozfun.map.get_key(event_params, 'product').string_value, '') AS product,
    NULLIF(mozfun.map.get_key(event_params, 'platform').string_value, '') AS platform,
    NULLIF(mozfun.map.get_key(event_params, 'release_channel').string_value, '') AS release_channel,
    NULLIF(
      mozfun.map.get_key(event_params, 'download_language').string_value,
      ''
    ) AS download_language,
    NULLIF(mozfun.map.get_key(event_params, 'method').string_value, '') AS method
  FROM
    `moz-fx-data-marketing-prod.analytics_489412379.events_*`
  WHERE
    _TABLE_SUFFIX = FORMAT_DATE('%Y%m%d', @submission_date)
    AND event_name = 'product_download'
),
sessions AS (
  SELECT
    ga_client_id,
    ga_session_id,
    landing_screen,
    manual_source,
    manual_medium,
    manual_campaign_name,
    ad_crosschannel_default_channel_group,
    ad_crosschannel_source_platform
  FROM
    `moz-fx-data-shared-prod.firefoxdotcom_derived.ga_sessions_v2`
  WHERE
    session_date
    BETWEEN DATE_SUB(@submission_date, INTERVAL 1 DAY)
    AND @submission_date
  -- One row per session so the join cannot multiply downloads.
  QUALIFY
    ROW_NUMBER() OVER (
      PARTITION BY
        ga_client_id,
        ga_session_id
      ORDER BY
        session_start_timestamp
    ) = 1
),
joined AS (
  SELECT
    d.page_location,
    d.country,
    d.product,
    d.platform,
    d.release_channel,
    d.download_language,
    d.method,
    s.landing_screen,
    s.manual_source AS source,
    s.manual_medium AS medium,
    s.manual_campaign_name AS campaign,
    s.ad_crosschannel_default_channel_group AS channel_group,
    s.ad_crosschannel_source_platform AS ad_platform
  FROM
    downloads AS d
  LEFT JOIN
    sessions AS s
    ON d.ga_client_id = s.ga_client_id
    AND d.ga_session_id = s.ga_session_id
),
with_paths AS (
  SELECT
    *,
    LOWER(RTRIM(REGEXP_EXTRACT(page_location, r'^https?://([^/?#]+)'), '.')) AS hostname,
    REGEXP_REPLACE(
      REGEXP_REPLACE(page_location, r'^https?://[^/?#]+', ''),
      r'[?#].*$',
      ''
    ) AS download_full_path,
    REGEXP_REPLACE(
      REGEXP_REPLACE(landing_screen, r'^https?://[^/?#]+', ''),
      r'[?#].*$',
      ''
    ) AS landing_full_path
  FROM
    joined
),
with_pages AS (
  SELECT
    *,
    REGEXP_EXTRACT(
      landing_full_path,
      r'^/([A-Za-z]{2}(?:-[A-Za-z]{2,4})?)(?:/|$)'
    ) AS landing_locale,
    REGEXP_REPLACE(
      landing_full_path,
      r'^/[A-Za-z]{2}(?:-[A-Za-z]{2,4})?(?:/|$)',
      '/'
    ) AS landing_page,
    REGEXP_REPLACE(
      download_full_path,
      r'^/[A-Za-z]{2}(?:-[A-Za-z]{2,4})?(?:/|$)',
      '/'
    ) AS download_page
  FROM
    with_paths
),
classified AS (
  SELECT
    hostname,
    landing_locale,
    landing_page,
    CASE
      WHEN landing_page IS NULL
        THEN NULL
      WHEN landing_page = '/'
        THEN 'home'
      WHEN REGEXP_EXTRACT(landing_page, r'^/([^/]+)') IN (
          'whatsnew',
          'thanks',
          'landing',
          'download',
          'mobile',
          'browsers',
          'features',
          'channel',
          'firefox',
          'privacy'
        )
        THEN REGEXP_EXTRACT(landing_page, r'^/([^/]+)')
      ELSE 'other'
    END AS landing_page_type,
    CASE
      WHEN download_page IS NULL
        THEN NULL
      WHEN download_page = '/'
        THEN 'home'
      WHEN REGEXP_EXTRACT(download_page, r'^/([^/]+)') IN (
          'whatsnew',
          'thanks',
          'landing',
          'download',
          'mobile',
          'browsers',
          'features',
          'channel',
          'firefox',
          'privacy'
        )
        THEN REGEXP_EXTRACT(download_page, r'^/([^/]+)')
      ELSE 'other'
    END AS download_page_type,
    country,
    source,
    medium,
    campaign,
    channel_group,
    ad_platform,
    product,
    platform,
    release_channel,
    download_language,
    method
  FROM
    with_pages
)
SELECT
  @submission_date AS event_date,
  hostname,
  landing_locale,
  landing_page,
  landing_page_type,
  download_page_type,
  country,
  source,
  medium,
  campaign,
  channel_group,
  ad_platform,
  product,
  platform,
  release_channel,
  download_language,
  method,
  COUNT(*) AS download_count
FROM
  classified
GROUP BY
  event_date,
  hostname,
  landing_locale,
  landing_page,
  landing_page_type,
  download_page_type,
  country,
  source,
  medium,
  campaign,
  channel_group,
  ad_platform,
  product,
  platform,
  release_channel,
  download_language,
  method
