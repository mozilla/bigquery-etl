WITH newtab_events_unnested AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    mozfun.norm.browser_version_info(client_info.app_display_version).major_version AS app_version,
    normalized_channel AS channel,
    metrics.string.newtab_locale AS locale,
    normalized_country_code AS country,
    metrics.string.newtab_content_surface_id AS newtab_content_surface_id,
    timestamp AS event_timestamp,
    category AS event_category,
    name AS event_name,
    extra AS event_details,
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_stable.newtab_v1`,
    UNNEST(events)
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND category IN ('pocket')
    AND name IN ('impression', 'click', 'dismiss')
    AND mozfun.norm.browser_version_info(
      client_info.app_display_version
    ).major_version >= 121 -- the [Pocket team started using Glean](https://github.com/Pocket/dbt-snowflake/pull/459) from this version on. This prevents duplicates for previous releases.
),
newtab_flattened_events AS (
  SELECT
    submission_date,
    SAFE_CAST(app_version AS INT64) AS app_version,
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
      mozfun.map.get_key(event_details, 'is_section_followed') AS BOOLEAN
    ) AS is_section_followed,
    mozfun.map.get_key(event_details, 'matches_selected_topic') AS matches_selected_topic,
    SAFE_CAST(mozfun.map.get_key(event_details, 'received_rank') AS INT64) AS received_rank,
    mozfun.map.get_key(event_details, 'section') AS section,
    SAFE_CAST(mozfun.map.get_key(event_details, 'section_position') AS INT64) AS section_position,
    mozfun.map.get_key(event_details, 'topic') AS topic,
    IFNULL(mozfun.map.get_key(event_details, 'content_redacted'), 'false') AS content_redacted,
    NULL AS newtab_content_ping_version
  FROM
    newtab_events_unnested
),
newtab_daily_agg AS (
  SELECT
    submission_date,
    app_version,
    channel,
    country,
    IFNULL(
      newtab_content_surface_id,
      mozfun.newtab.scheduled_surface_id_v1(country, locale)
    ) AS newtab_content_surface_id,
    corpus_item_id,
    position,
    is_sponsored,
    is_section_followed,
    matches_selected_topic,
    received_rank,
    section,
    section_position,
    topic,
    content_redacted,
    newtab_content_ping_version,
    COUNTIF(event_name = 'impression') AS impression_count,
    COUNTIF(event_name = 'click') AS click_count,
    COUNTIF(event_name = 'dismiss') AS dismiss_count
  FROM
    newtab_flattened_events
  WHERE
    -- Filters out non-redacted events. Redacted events will be counted in the newtab_content ping data.
    content_redacted = 'false'
  GROUP BY
    submission_date,
    app_version,
    channel,
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
    content_redacted,
    newtab_content_ping_version
),
newtab_content_events_unnested AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    normalized_channel AS channel,
    IFNULL(metrics.string.newtab_content_country, normalized_country_code) AS country,
    metrics.string.newtab_content_surface_id AS newtab_content_surface_id,
    timestamp AS event_timestamp,
    category AS event_category,
    name AS event_name,
    extra AS event_details,
    metrics.quantity.newtab_content_ping_version AS newtab_content_ping_version
  FROM
    `moz-fx-data-shared-prod.firefox_desktop.newtab_content`,
    UNNEST(events)
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND category IN ('newtab_content')
    AND name IN ('impression', 'click', 'dismiss')
),
newtab_content_flattened_events AS (
  SELECT
    submission_date,
    NULL AS app_version,
    channel,
    country,
    newtab_content_surface_id,
    event_category,
    event_name,
    mozfun.map.get_key(event_details, 'corpus_item_id') AS corpus_item_id,
    SAFE_CAST(mozfun.map.get_key(event_details, 'position') AS INT64) AS position,
    SAFE_CAST(mozfun.map.get_key(event_details, 'is_sponsored') AS BOOLEAN) AS is_sponsored,
    SAFE_CAST(
      mozfun.map.get_key(event_details, 'is_section_followed') AS BOOLEAN
    ) AS is_section_followed,
    mozfun.map.get_key(event_details, 'matches_selected_topic') AS matches_selected_topic,
    SAFE_CAST(mozfun.map.get_key(event_details, 'received_rank') AS INT64) AS received_rank,
    mozfun.map.get_key(event_details, 'section') AS section,
    SAFE_CAST(mozfun.map.get_key(event_details, 'section_position') AS INT64) AS section_position,
    mozfun.map.get_key(event_details, 'topic') AS topic,
    CAST(NULL AS STRING) AS content_redacted,
    newtab_content_ping_version
  FROM
    newtab_content_events_unnested
),
newtab_content_daily_agg AS (
  SELECT
    submission_date,
    app_version,
    channel,
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
    content_redacted,
    newtab_content_ping_version,
    COUNTIF(event_name = 'impression') AS impression_count,
    COUNTIF(event_name = 'click') AS click_count,
    COUNTIF(event_name = 'dismiss') AS dismiss_count
  FROM
    newtab_content_flattened_events
  WHERE
    -- Only including events from pings with a version
    -- to ensure all events coming from this CTE are from the Newtab-Content ping
    newtab_content_ping_version IS NOT NULL
  GROUP BY
    submission_date,
    app_version,
    channel,
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
    content_redacted,
    newtab_content_ping_version
)
SELECT
  *
FROM
  newtab_daily_agg
UNION ALL
SELECT
  *
FROM
  newtab_content_daily_agg
