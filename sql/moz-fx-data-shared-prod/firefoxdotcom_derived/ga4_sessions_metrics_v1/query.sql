-- Daily rollup of firefox.com sessions from ga_sessions_v2 by landing page, country, device and traffic source.
WITH sessions AS (
  SELECT
    landing_screen,
    country,
    device_category,
    manual_source AS source,
    manual_medium AS medium,
    manual_campaign_name AS campaign,
    ad_crosschannel_default_channel_group AS channel_group,
    ad_crosschannel_source_platform AS ad_platform,
    engaged_session,
    had_download_event,
    firefox_desktop_downloads,
    pageviews,
    time_on_site
  FROM
    `moz-fx-data-shared-prod.firefoxdotcom_derived.ga_sessions_v2`
  WHERE
    session_date = @submission_date
  -- One row per session.
  QUALIFY
    ROW_NUMBER() OVER (
      PARTITION BY
        ga_client_id,
        ga_session_id
      ORDER BY
        session_start_timestamp
    ) = 1
),
with_paths AS (
  SELECT
    *,
    LOWER(RTRIM(REGEXP_EXTRACT(landing_screen, r'^https?://([^/?#]+)'), '.')) AS hostname,
    REGEXP_REPLACE(
      REGEXP_REPLACE(landing_screen, r'^https?://[^/?#]+', ''),
      r'[?#].*$',
      ''
    ) AS landing_full_path
  FROM
    sessions
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
    ) AS landing_page
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
    country,
    device_category,
    source,
    medium,
    campaign,
    channel_group,
    ad_platform,
    engaged_session,
    had_download_event,
    firefox_desktop_downloads,
    pageviews,
    time_on_site
  FROM
    with_pages
)
SELECT
  @submission_date AS session_date,
  hostname,
  landing_locale,
  landing_page,
  landing_page_type,
  country,
  device_category,
  source,
  medium,
  campaign,
  channel_group,
  ad_platform,
  COUNT(*) AS sessions,
  COALESCE(SUM(engaged_session), 0) AS engaged_sessions,
  COUNTIF(had_download_event) AS sessions_with_download,
  COALESCE(SUM(firefox_desktop_downloads), 0) AS firefox_desktop_downloads,
  COALESCE(SUM(pageviews), 0) AS total_pageviews,
  COALESCE(SUM(time_on_site), 0) AS total_time_on_site_seconds
FROM
  classified
GROUP BY
  session_date,
  hostname,
  landing_locale,
  landing_page,
  landing_page_type,
  country,
  device_category,
  source,
  medium,
  campaign,
  channel_group,
  ad_platform
