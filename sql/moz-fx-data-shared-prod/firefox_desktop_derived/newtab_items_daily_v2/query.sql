WITH events_unnested AS (
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
    DATE(submission_timestamp) = @submission_date
    AND category IN ('pocket')
    AND name IN ('impression', 'click', 'save', 'dismiss')
    AND mozfun.norm.browser_version_info(
      client_info.app_display_version
    ).major_version >= 121 -- the [Pocket team started using Glean](https://github.com/Pocket/dbt-snowflake/pull/459) from this version on. This prevents duplicates for previous releases.
),
flattened_events AS (
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
    mozfun.map.get_key(event_details, 'format') AS format,
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
),
/* Find default propensity weight for long-tail positions not in the propensity table */
default_propensity AS (
  SELECT
    COALESCE(
      (
        SELECT
          AVG(wt.weight)
        FROM
          `moz-fx-data-shared-prod.telemetry_derived.newtab_merino_propensity_v1` wt
        WHERE
          position > 80
      ),
      1.0
    ) AS default_weight
),
aggregated AS (
  SELECT
    submission_date,
    app_version,
    channel,
    locale,
    country,
    mozfun.newtab.scheduled_surface_id_v1(country, locale) AS scheduled_surface_id,
    corpus_item_id,
    position,
    format,
    is_sponsored,
    is_section_followed,
    matches_selected_topic,
    received_rank,
    section,
    section_position,
    flattened_events.topic,
    ANY_VALUE(corpus_items.title) AS title,
    ANY_VALUE(corpus_items.url) AS recommendation_url,
    ANY_VALUE(corpus_items.authors) AS authors,
    ANY_VALUE(corpus_items.publisher) AS publisher,
    COUNTIF(event_name = 'impression') AS impression_count,
    COUNTIF(event_name = 'click') AS click_count,
    COUNTIF(event_name = 'save') AS save_count,
    COUNTIF(event_name = 'dismiss') AS dismiss_count
  FROM
    flattened_events
  LEFT OUTER JOIN
    `moz-fx-data-shared-prod.snowflake_migration_derived.corpus_items_updated_v1` AS corpus_items
    ON flattened_events.corpus_item_id = corpus_items.approved_corpus_item_external_id
  GROUP BY
    submission_date,
    app_version,
    channel,
    locale,
    country,
    scheduled_surface_id,
    corpus_item_id,
    position,
    format,
    is_sponsored,
    is_section_followed,
    matches_selected_topic,
    received_rank,
    section,
    section_position,
    topic
)
SELECT
  aggregated.submission_date,
  aggregated.app_version,
  aggregated.channel,
  aggregated.locale,
  aggregated.country,
  aggregated.scheduled_surface_id,
  aggregated.corpus_item_id,
  aggregated.position,
  aggregated.format,
  aggregated.is_sponsored,
  aggregated.is_section_followed,
  aggregated.matches_selected_topic,
  aggregated.received_rank,
  aggregated.section,
  aggregated.section_position,
  aggregated.topic,
  aggregated.title,
  aggregated.recommendation_url,
  aggregated.authors,
  aggregated.publisher,
  aggregated.impression_count,
  aggregated.click_count,
  aggregated.save_count,
  aggregated.dismiss_count,
  /* Apply propensity correction to section impressions only.
     Section items are ranked by a content-aware algorithm, so their position-based
     exposure rate differs from a uniform random placement. Dividing by the positional
     propensity weight normalises impressions to be comparable across positions.
     Non-section (grid) impressions are passed through unchanged. */
  CASE
    WHEN aggregated.section_position IS NOT NULL
      THEN SAFE_CAST(
          aggregated.impression_count / COALESCE(
            propensity.weight,
            default_propensity.default_weight
          ) AS INT64
        )
    ELSE aggregated.impression_count
  END AS impression_count_adjusted
FROM
  aggregated
CROSS JOIN
  default_propensity
LEFT JOIN
  `moz-fx-data-shared-prod.telemetry_derived.newtab_merino_propensity_v1` AS propensity
  ON propensity.position = aggregated.position
  AND propensity.tile_format = aggregated.format
