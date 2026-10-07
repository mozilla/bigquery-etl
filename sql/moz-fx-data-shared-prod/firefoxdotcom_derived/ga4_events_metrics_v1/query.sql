-- Daily rollup of GA4 events on firefox.com by event, page, country and selected event parameters.
WITH events AS (
  SELECT
    event_name AS raw_event_name,
    geo.country AS country,
    (
      SELECT
        `value`
      FROM
        UNNEST(event_params)
      WHERE
        key = 'page_location'
    ).string_value AS page_location,
    (
      SELECT
        `value`
      FROM
        UNNEST(event_params)
      WHERE
        key = 'engagement_time_msec'
    ).int_value AS engagement_time_msec,
    -- Blank strings become NULL.
    NULLIF((SELECT `value` FROM UNNEST(event_params) WHERE key = 'uid').string_value, '') AS uid,
    NULLIF(
      (SELECT `value` FROM UNNEST(event_params) WHERE key = 'type').string_value,
      ''
    ) AS `type`,
    NULLIF(
      (SELECT `value` FROM UNNEST(event_params) WHERE key = 'position').string_value,
      ''
    ) AS position,
    NULLIF(
      (SELECT `value` FROM UNNEST(event_params) WHERE key = 'link_id').string_value,
      ''
    ) AS link_id,
    NULLIF(
      (SELECT `value` FROM UNNEST(event_params) WHERE key = 'link_classes').string_value,
      ''
    ) AS link_classes,
    NULLIF(
      (SELECT `value` FROM UNNEST(event_params) WHERE key = 'link_url').string_value,
      ''
    ) AS link_url,
    NULLIF((SELECT `value` FROM UNNEST(event_params) WHERE key = 'text').string_value, '') AS text,
    NULLIF(
      (SELECT `value` FROM UNNEST(event_params) WHERE key = 'gtm_tag_name').string_value,
      ''
    ) AS gtm_tag_name,
    NULLIF(
      (SELECT `value` FROM UNNEST(event_params) WHERE key = 'product').string_value,
      ''
    ) AS product,
    NULLIF(
      (SELECT `value` FROM UNNEST(event_params) WHERE key = 'platform').string_value,
      ''
    ) AS platform,
    NULLIF(
      (SELECT `value` FROM UNNEST(event_params) WHERE key = 'release_channel').string_value,
      ''
    ) AS release_channel,
    NULLIF(
      (SELECT `value` FROM UNNEST(event_params) WHERE key = 'download_language').string_value,
      ''
    ) AS download_language,
    NULLIF(
      (SELECT `value` FROM UNNEST(event_params) WHERE key = 'method').string_value,
      ''
    ) AS method,
    NULLIF(
      (SELECT `value` FROM UNNEST(event_params) WHERE key = 'action').string_value,
      ''
    ) AS action,
    NULLIF((SELECT `value` FROM UNNEST(event_params) WHERE key = 'name').string_value, '') AS name,
    NULLIF(
      (SELECT `value` FROM UNNEST(event_params) WHERE key = 'newsletter_id').string_value,
      ''
    ) AS newsletter_id,
    (
      SELECT
        `value`
      FROM
        UNNEST(event_params)
      WHERE
        key = 'percent_scrolled'
    ).int_value AS percent_scrolled
  FROM
    `moz-fx-data-marketing-prod.analytics_489412379.events_*`
  WHERE
    _TABLE_SUFFIX = FORMAT_DATE('%Y%m%d', @submission_date)
),
with_path AS (
  SELECT
    *,
    LOWER(RTRIM(REGEXP_EXTRACT(page_location, r'^https?://([^/?#]+)'), '.')) AS hostname,
    REGEXP_REPLACE(
      REGEXP_REPLACE(page_location, r'^https?://[^/?#]+', ''),
      r'[?#].*$',
      ''
    ) AS full_path
  FROM
    events
),
with_page AS (
  SELECT
    *,
    REGEXP_EXTRACT(full_path, r'^/([A-Za-z]{2}(?:-[A-Za-z]{2,4})?)(?:/|$)') AS locale,
    REGEXP_REPLACE(full_path, r'^/[A-Za-z]{2}(?:-[A-Za-z]{2,4})?(?:/|$)', '/') AS page_path
  FROM
    with_path
),
classified AS (
  SELECT
    -- Names outside the allowlist are grouped as 'other'.
    IF(
      raw_event_name IN (
        'page_view',
        'session_start',
        'stub_session_set',
        'user_engagement',
        'first_visit',
        'cta_click',
        'scroll',
        'product_download',
        'firefox_download',
        'firefox_mobile_download',
        'focus_download',
        'widget_action',
        'link_click',
        'newsletter_subscribe',
        'dimension_set',
        'send_to_device',
        'default_browser_set'
      ),
      raw_event_name,
      'other'
    ) AS event_name,
    hostname,
    locale,
    page_path,
    CASE
      WHEN page_path IS NULL
        THEN NULL
      WHEN page_path = '/'
        THEN 'home'
      WHEN REGEXP_EXTRACT(page_path, r'^/([^/]+)') IN (
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
        THEN REGEXP_EXTRACT(page_path, r'^/([^/]+)')
      ELSE 'other'
    END AS page_type,
    CASE
      WHEN REGEXP_EXTRACT(page_path, r'^/whatsnew/([^/]+)') = 'general'
        THEN REGEXP_EXTRACT(page_location, r'[?&]version=([^&#]+)')
      ELSE REGEXP_EXTRACT(page_path, r'^/whatsnew/([^/]+)')
    END AS whatsnew_version,
    country,
    -- Parameters are only filled for the events that own them.
    IF(raw_event_name = 'cta_click', uid, NULL) AS cta_uid,
    IF(raw_event_name = 'cta_click', `type`, NULL) AS cta_type,
    IF(raw_event_name IN ('cta_click', 'link_click'), position, NULL) AS link_position,
    IF(raw_event_name IN ('cta_click', 'link_click'), link_id, NULL) AS link_id,
    IF(raw_event_name IN ('cta_click', 'link_click'), link_classes, NULL) AS link_classes,
    IF(raw_event_name IN ('cta_click', 'link_click'), text, NULL) AS link_text,
    -- Drop query string and fragment.
    IF(
      raw_event_name IN ('cta_click', 'link_click'),
      REGEXP_REPLACE(link_url, r'[?#].*$', ''),
      NULL
    ) AS link_url,
    IF(raw_event_name IN ('cta_click', 'link_click'), gtm_tag_name, NULL) AS gtm_tag_name,
    IF(
      raw_event_name IN (
        'product_download',
        'firefox_download',
        'firefox_mobile_download',
        'focus_download'
      ),
      product,
      NULL
    ) AS product,
    IF(
      raw_event_name IN (
        'product_download',
        'firefox_download',
        'firefox_mobile_download',
        'focus_download'
      ),
      platform,
      NULL
    ) AS platform,
    IF(
      raw_event_name IN (
        'product_download',
        'firefox_download',
        'firefox_mobile_download',
        'focus_download'
      ),
      release_channel,
      NULL
    ) AS release_channel,
    IF(
      raw_event_name IN (
        'product_download',
        'firefox_download',
        'firefox_mobile_download',
        'focus_download'
      ),
      download_language,
      NULL
    ) AS download_language,
    IF(
      raw_event_name IN (
        'product_download',
        'firefox_download',
        'firefox_mobile_download',
        'focus_download',
        'send_to_device'
      ),
      method,
      NULL
    ) AS method,
    IF(raw_event_name = 'widget_action', `type`, NULL) AS widget_type,
    IF(raw_event_name = 'widget_action', action, NULL) AS widget_action,
    IF(raw_event_name = 'widget_action', name, NULL) AS widget_name,
    IF(raw_event_name = 'newsletter_subscribe', newsletter_id, NULL) AS newsletter_id,
    IF(raw_event_name = 'scroll', percent_scrolled, NULL) AS percent_scrolled,
    engagement_time_msec
  FROM
    with_page
)
SELECT
  @submission_date AS event_date,
  event_name,
  hostname,
  locale,
  page_path,
  page_type,
  whatsnew_version,
  country,
  cta_uid,
  cta_type,
  link_position,
  link_id,
  link_classes,
  link_text,
  link_url,
  gtm_tag_name,
  product,
  platform,
  release_channel,
  download_language,
  method,
  widget_type,
  widget_action,
  widget_name,
  newsletter_id,
  percent_scrolled,
  COUNT(*) AS event_count,
  -- Each event is capped at 30 minutes before summing.
  COALESCE(SUM(LEAST(engagement_time_msec, 1800000)), 0) AS total_engagement_time_ms
FROM
  classified
GROUP BY
  event_date,
  event_name,
  hostname,
  locale,
  page_path,
  page_type,
  whatsnew_version,
  country,
  cta_uid,
  cta_type,
  link_position,
  link_id,
  link_classes,
  link_text,
  link_url,
  gtm_tag_name,
  product,
  platform,
  release_channel,
  download_language,
  method,
  widget_type,
  widget_action,
  widget_name,
  newsletter_id,
  percent_scrolled
