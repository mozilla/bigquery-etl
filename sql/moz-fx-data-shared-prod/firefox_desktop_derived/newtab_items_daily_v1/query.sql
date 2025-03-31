WITH events_dedupe AS (
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_stable.newtab_v1`
  WHERE
    DATE(submission_timestamp) = @submission_date
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY document_id ORDER BY submission_timestamp) = 1
),
events_unnested AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    mozfun.norm.browser_version_info(client_info.app_display_version).major_version AS app_version,
    normalized_channel AS channel,
    metrics.string.newtab_locale AS locale,
    normalized_country_code AS country,
    timestamp AS event_timestamp,
    category AS event_category,
    name AS event_name,
    extra AS event_details,
  FROM
    events_dedupe,
    UNNEST(events)
  WHERE
    category IN ('pocket')
    AND name IN ('impression', 'click', 'save', 'dismiss')
    AND mozfun.norm.browser_version_info(client_info.app_display_version).major_version >= 121
),
flattened_events AS (
  SELECT
    submission_date,
    app_version,
    channel,
    locale,
    country,
    event_category,
    event_name,
    mozfun.map.get_key(event_details, 'corpus_item_id') AS corpus_item_id,
    SAFE_CAST(mozfun.map.get_key(event_details, 'position') AS INT64) AS position,
    SAFE_CAST(mozfun.map.get_key(event_details, 'is_sponsored') AS BOOLEAN) AS is_sponsored,
    SAFE_CAST(
      mozfun.map.get_key(event_details, 'is_secton_followed') AS BOOLEAN
    ) AS is_secton_followed,
    mozfun.map.get_key(event_details, 'matches_selected_topic') AS matches_selected_topic,
    mozfun.map.get_key(event_details, 'newtab_visit_id') AS newtab_visit_id,
    SAFE_CAST(mozfun.map.get_key(event_details, 'received_rank') AS INT64) AS received_rank,
    mozfun.map.get_key(event_details, 'section') AS section,
    mozfun.map.get_key(event_details, 'section_position') AS section_position,
    mozfun.map.get_key(event_details, 'topic') AS topic,
  FROM
    events_unnested
)
SELECT
  submission_date,
  app_version,
  channel,
  locale,
  country,
  CASE
    WHEN SPLIT(locale, '-')[0] = 'de'
      THEN 'NEW_TAB_DE_DE'
    WHEN SPLIT(locale, '-')[0] = 'es'
      THEN 'NEW_TAB_ES_ES'
    WHEN SPLIT(locale, '-')[0] = 'fr'
      THEN 'NEW_TAB_FR_FR'
    WHEN SPLIT(locale, '-')[0] = 'it'
      THEN 'NEW_TAB_IT_IT'
    WHEN SPLIT(locale, '-')[0] = 'en'
      AND (SPLIT(locale, '-')[1] IN ('GB', 'IE') OR country IN ('GB', 'IE'))
      THEN 'NEW_TAB_EN_GB'
    WHEN SPLIT(locale, '-')[0] = 'en'
      AND (SPLIT(locale, '-')[1] = 'IN' OR country = 'IN')
      THEN 'NEW_TAB_EN_INTL'
    WHEN SPLIT(locale, '-')[0] = 'en'
      AND (SPLIT(locale, '-')[1] IN ('US', 'CA') OR country IN ('US', 'CA'))
      THEN 'NEW_TAB_EN_US'
    ELSE 'NEW_TAB_EN_US'
  END AS scheduled_surface_id,
  corpus_item_id,
  position,
  is_sponsored,
  is_secton_followed,
  matches_selected_topic,
  received_rank,
  section,
  section_position,
  topic,
  SUM(IF(event_name = 'impression', 1, 0)) AS impression_count,
  SUM(IF(event_name = 'click', 1, 0)) AS click_count,
  SUM(IF(event_name = 'save', 1, 0)) AS save_count,
  SUM(IF(event_name = 'dismiss', 1, 0)) AS dismiss_count,
FROM
  flattened_events
WHERE
  section IS NOT NULL
GROUP BY
  submission_date,
  app_version,
  channel,
  locale,
  country,
  scheduled_surface_id,
  corpus_item_id,
  position,
  is_sponsored,
  is_secton_followed,
  matches_selected_topic,
  received_rank,
  section,
  section_position,
  topic
