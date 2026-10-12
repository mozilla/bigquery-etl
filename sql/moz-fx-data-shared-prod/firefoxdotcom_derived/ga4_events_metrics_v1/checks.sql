#fail
{{ min_row_count(1, "event_date = @submission_date") }}

#fail
{{ not_null(["event_date", "event_name"], "event_date = @submission_date") }}

#fail
{{ is_unique(["event_date", "event_name", "hostname", "locale", "page_path", "page_type", "whatsnew_version", "country", "cta_uid", "cta_type", "link_position", "link_id", "link_classes", "link_text", "link_url", "gtm_tag_name", "product", "platform", "release_channel", "download_language", "method", "widget_type", "widget_action", "widget_name", "newsletter_id", "percent_scrolled"], "event_date = @submission_date") }}
-- Warns on event_params keys that are neither promoted nor on the ignore list.

#warn
WITH unexpected_keys AS (
  SELECT
    p.key AS clean_key,
    COUNT(*) AS record_count
  FROM
    `moz-fx-data-marketing-prod.analytics_489412379.events_*`,
    UNNEST(event_params) AS p
  WHERE
    _TABLE_SUFFIX = FORMAT_DATE('%Y%m%d', @submission_date)
    AND p.key NOT IN (
      -- Promoted in query.sql
      'page_location',
      'engagement_time_msec',
      'uid',
      'type',
      'position',
      'link_id',
      'link_classes',
      'link_url',
      'gtm_tag_name',
      'text',
      'product',
      'platform',
      'release_channel',
      'download_language',
      'method',
      'action',
      'newsletter_id',
      'percent_scrolled',
      'name',
      -- Intentionally not promoted
      'batch_ordering_id',
      'batch_page_id',
      'ga_session_number',
      'ga_session_id',
      'session_engaged',
      'engaged_session_event',
      'page_title',
      'page_referrer',
      'firebase_conversion',
      'ignore_referrer',
      'debug_mode',
      'entrances',
      'source',
      'medium',
      'campaign',
      'campaign_id',
      'term',
      'content',
      'gclid',
      'gclsrc',
      'gad_source',
      'gad_campaignid',
      'msclkid',
      'fbclid',
      'twclid',
      'rdt_cid',
      'id'
    )
  GROUP BY
    clean_key
  HAVING
    record_count >= 100
)
SELECT
  IF(
    (SELECT COUNT(*) FROM unexpected_keys) > 0,
    ERROR(
      FORMAT(
        'Unmapped event_params key(s) found on %t -- consider promoting to a column in firefoxdotcom_derived.ga4_events_metrics_v1 or adding to the ignore list in checks.sql: %t',
        @submission_date,
        ARRAY(
          SELECT AS STRUCT
            clean_key AS `key`,
            record_count
          FROM
            unexpected_keys
          ORDER BY
            record_count DESC
        )
      )
    ),
    NULL
  );

-- Warns on event names grouped as 'other' with meaningful volume.

#warn
WITH unexpected_events AS (
  SELECT
    event_name,
    COUNT(*) AS record_count
  FROM
    `moz-fx-data-marketing-prod.analytics_489412379.events_*`
  WHERE
    _TABLE_SUFFIX = FORMAT_DATE('%Y%m%d', @submission_date)
    AND event_name NOT IN (
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
    )
  GROUP BY
    event_name
  HAVING
    record_count >= 100
)
SELECT
  IF(
    (SELECT COUNT(*) FROM unexpected_events) > 0,
    ERROR(
      FORMAT(
        'Event name(s) bucketed as other with meaningful volume on %t -- consider adding to the allowlist in firefoxdotcom_derived.ga4_events_metrics_v1/query.sql: %t',
        @submission_date,
        ARRAY(
          SELECT AS STRUCT
            event_name,
            record_count
          FROM
            unexpected_events
          ORDER BY
            record_count DESC
        )
      )
    ),
    NULL
  );
