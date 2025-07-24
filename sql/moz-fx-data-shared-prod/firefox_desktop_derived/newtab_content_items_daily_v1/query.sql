WITH events_unnested AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    -- mozfun.norm.browser_version_info(client_info.app_display_version).major_version AS app_version,
    140 AS app_version,  -- Placeholder for app_version, adjust as needed
    normalized_channel AS channel,
    -- metrics.string.newtab_locale AS locale,
    "MISSING" AS locale,  -- Placeholder for locale, adjust as needed
    normalized_country_code AS country,
    metrics.string.newtab_content_surface_id AS newtab_content_surface_id,
    timestamp AS event_timestamp,
    category AS event_category,
    name AS event_name,
    extra AS event_details,
  FROM
    `moz-fx-data-shared-prod.firefox_desktop.newtab_content`,
    UNNEST(events)
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND category IN ('newtab_content')
    AND name IN ('impression', 'click', 'dismiss')
    -- AND mozfun.norm.browser_version_info(
    --   client_info.app_display_version
    -- ).major_version >= 140 -- Transitioned to the newtab-content ping in version 140, so we only want to include events after that version for this ping.
),
flattened_events AS (
  SELECT
    submission_date,
    -- SAFE_CAST(app_version AS INT64) AS app_version,
    app_version,  -- Use the placeholder directly
    channel,
    locale,
    country,
    newtab_content_surface_id,
    event_category,
    event_name,
    mozfun.map.get_key(event_details, 'corpus_item_id') AS corpus_item_id,
    SAFE_CAST(mozfun.map.get_key(event_details, 'position') AS INT64) AS position,
    SAFE_CAST(mozfun.map.get_key(event_details, 'is_sponsored') AS BOOLEAN) AS is_sponsored,
    SAFE_CAST(
      mozfun.map.get_key(event_details, 'is_secton_followed') AS BOOLEAN
    ) AS is_section_followed,
    mozfun.map.get_key(event_details, 'matches_selected_topic') AS matches_selected_topic,
    mozfun.map.get_key(event_details, 'newtab_visit_id') AS newtab_visit_id,
    SAFE_CAST(mozfun.map.get_key(event_details, 'received_rank') AS INT64) AS received_rank,
    mozfun.map.get_key(event_details, 'section') AS section,
    SAFE_CAST(mozfun.map.get_key(event_details, 'section_position') AS INT64) AS section_position,
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
  newtab_content_surface_id,
  corpus_item_id,
  position,
  is_sponsored,
  is_section_followed,
  matches_selected_topic,
  received_rank,
  section,
  section_position,
  topic,
  COUNTIF(event_name = 'impression') AS impression_count,
  COUNTIF(event_name = 'click') AS click_count,
  COUNTIF(event_name = 'save') AS save_count,
  COUNTIF(event_name = 'dismiss') AS dismiss_count
FROM
  flattened_events
GROUP BY
  submission_date,
  app_version,
  channel,
  locale,
  country,
  newtab_content_surface_id,
  corpus_item_id,
  position,
  is_sponsored,
  is_section_followed,
  matches_selected_topic,
  received_rank,
  section,
  section_position,
  topic
