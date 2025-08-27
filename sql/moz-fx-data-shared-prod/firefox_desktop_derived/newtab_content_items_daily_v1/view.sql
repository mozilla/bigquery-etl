CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_desktop_derived.newtab_content_items_daily_v1`
AS
WITH newtab_events_unnested AS (
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
    `moz-fx-data-shared-prod.firefox_desktop_stable.newtab_v1`,
    UNNEST(events)
  WHERE
    category IN ('pocket')
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
    event_category,
    event_name,
    mozfun.map.get_key(event_details, 'corpus_item_id') AS corpus_item_id,
    SAFE_CAST(mozfun.map.get_key(event_details, 'position') AS INT64) AS position,
    SAFE_CAST(mozfun.map.get_key(event_details, 'is_sponsored') AS BOOLEAN) AS is_sponsored,
    SAFE_CAST(
      mozfun.map.get_key(event_details, 'is_section_followed') AS BOOLEAN
    ) AS is_section_followed,
    mozfun.map.get_key(event_details, 'matches_selected_topic') AS matches_selected_topic,
    mozfun.map.get_key(event_details, 'newtab_visit_id') AS newtab_visit_id,
    SAFE_CAST(mozfun.map.get_key(event_details, 'received_rank') AS INT64) AS received_rank,
    mozfun.map.get_key(event_details, 'section') AS section,
    SAFE_CAST(mozfun.map.get_key(event_details, 'section_position') AS INT64) AS section_position,
    mozfun.map.get_key(event_details, 'topic') AS topic,
    mozfun.map.get_key(event_details, 'content_redacted') AS content_redacted,
    NULL AS newtab_content_ping_version
  FROM
    newtab_events_unnested
),
newtab_daily_agg AS (
  SELECT
    submission_date,
    channel,
    country,
    '' AS newtab_content_surface_id,
    newtab_flattened_events.corpus_item_id,
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
    corpus_items.title AS title,
    corpus_items.url AS recommendation_url,
    corpus_items.authors AS authors,
    corpus_items.publisher AS publisher,
    COUNTIF(event_name = 'impression') AS impression_count,
    COUNTIF(event_name = 'click') AS click_count,
    COUNTIF(event_name = 'dismiss') AS dismiss_count
  FROM
    newtab_flattened_events
  LEFT OUTER JOIN
    `moz-fx-data-shared-prod.snowflake_migration_derived.corpus_items_current_v1` AS corpus_items
    ON newtab_flattened_events.corpus_item_id = corpus_items.corpus_item_id
  WHERE
    content_redacted = 'FALSE'
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
    newtab_content_ping_version,
    title,
    recommendation_url,
    authors,
    publisher
),
newtab_content_events_unnested AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    normalized_channel AS channel,
    normalized_country_code AS country,
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
    category IN ('newtab_content')
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
    '' AS content_redacted,
    newtab_content_ping_version
  FROM
    newtab_content_events_unnested
),
newtab_content_daily_agg AS (
  SELECT
    submission_date,
    channel,
    country,
    newtab_content_surface_id,
    newtab_content_flattened_events.corpus_item_id,
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
    corpus_items.title AS title,
    corpus_items.url AS recommendation_url,
    corpus_items.authors AS authors,
    corpus_items.publisher AS publisher,
    COUNTIF(event_name = 'impression') AS impression_count,
    COUNTIF(event_name = 'click') AS click_count,
    COUNTIF(event_name = 'dismiss') AS dismiss_count
  FROM
    newtab_content_flattened_events
  LEFT OUTER JOIN
    `moz-fx-data-shared-prod.snowflake_migration_derived.corpus_items_current_v1` AS corpus_items
    ON newtab_content_flattened_events.corpus_item_id = corpus_items.corpus_item_id
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
    newtab_content_ping_version,
    title,
    recommendation_url,
    authors,
    publisher
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
