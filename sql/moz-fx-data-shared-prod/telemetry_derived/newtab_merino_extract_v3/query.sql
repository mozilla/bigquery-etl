WITH legacy_pings AS (
  SELECT
    submission_timestamp,
    document_id,
    events,
    normalized_country_code
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_live.newtab_v1`
  WHERE
    submission_timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
),
private_pings AS (
  SELECT
    submission_timestamp,
    document_id,
    events,
    metrics.string.newtab_content_country AS normalized_country_code
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_live.newtab_content_v1`
  WHERE
    submission_timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
),
combined_pings AS (
  SELECT
    *
  FROM
    legacy_pings
  UNION ALL
  SELECT
    *
  FROM
    private_pings
),
deduplicated_pings AS (
  SELECT
    *
  FROM
    combined_pings
  QUALIFY
    ROW_NUMBER() OVER (
      PARTITION BY
        DATE(submission_timestamp),
        document_id
      ORDER BY
        submission_timestamp DESC
    ) = 1
),
flattened_newtab_events AS (
  SELECT
    document_id,
    submission_timestamp,
    normalized_country_code,
    unnested_events.name AS event_name,
    mozfun.map.get_key(unnested_events.extra, 'corpus_item_id') AS corpus_item_id,
    SAFE_CAST(mozfun.map.get_key(unnested_events.extra, 'position') AS INT64) AS position,
    mozfun.map.get_key(unnested_events.extra, 'format') AS format,
    SAFE_CAST(
      mozfun.map.get_key(unnested_events.extra, 'section_position') AS INT64
    ) AS section_position
  FROM
    deduplicated_pings dp
  CROSS JOIN
    UNNEST(dp.events) AS unnested_events
  WHERE
    -- Filter to relevant events only
    unnested_events.category IN ('pocket', 'newtab_content')
    AND unnested_events.name IN ('impression', 'click', 'report_content_submit')
    -- Keep only rows with a non-null corpus_item_id
    AND mozfun.map.get_key(unnested_events.extra, 'corpus_item_id') IS NOT NULL
),
raw_grouped_totals AS (
  SELECT
    normalized_country_code,
    corpus_item_id,
    position,
    format,
    section_position,
    SUM(CASE WHEN event_name = 'impression' THEN 1 ELSE 0 END) AS raw_impression_count,
    SUM(CASE WHEN event_name = 'click' THEN 1 ELSE 0 END) AS click_count,
    SUM(CASE WHEN event_name = 'report_content_submit' THEN 1 ELSE 0 END) AS report_count,
  FROM
    flattened_newtab_events
  GROUP BY
    normalized_country_code,
    corpus_item_id,
    position,
    format,
    section_position
),
/* Separate and adjust section events */
section_events AS (
  SELECT
    rw.normalized_country_code,
    rw.corpus_item_id,
    rw.raw_impression_count,
    -- apply propensity scaling to impressions only
    rw.raw_impression_count / COALESCE(wt.weight, 0.2) AS adjusted_impression_count,
    rw.report_count,
    rw.click_count
  FROM
    raw_grouped_totals rw
  LEFT JOIN
    `moz-fx-data-shared-prod.telemetry_derived.newtab_merino_propensity_v1` wt
    ON SAFE_CAST(wt.position AS INT64) = rw.position
    AND wt.tile_format = rw.format
  WHERE
    rw.section_position IS NOT NULL
),
/* Separate non-section (grid) type events */
non_section_events AS (
  SELECT
    normalized_country_code,
    corpus_item_id,
    raw_impression_count,
    raw_impression_count AS adjusted_impression_count, -- pass through unchanged
    report_count,
    click_count
  FROM
    raw_grouped_totals
  WHERE
    section_position IS NULL
),
/* Re-join events into single table */
combined_events AS (
  SELECT
    *
  FROM
    non_section_events
  UNION ALL
  SELECT
    *
  FROM
    section_events
),
/* Aggregate clicks, impressions, and reports by corpus_item_id and normalized_country_code. */
aggregated_events AS (
  SELECT
    fe.corpus_item_id,
    fe.normalized_country_code,
    SAFE_CAST(SUM(adjusted_impression_count) AS INT64) AS impression_count,
    SUM(click_count) AS click_count,
    SUM(report_count) AS report_count
  FROM
    combined_events fe
  GROUP BY
    1,
    2
),
/* Aggregate clicks, impressions, and reports across all countries. */
global_aggregates AS (
  SELECT
    corpus_item_id,
    CAST(NULL AS STRING) AS region,
    SUM(impression_count) AS impression_count,
    SUM(click_count) AS click_count,
    SUM(report_count) AS report_count
  FROM
    aggregated_events
  GROUP BY
    corpus_item_id
),
/* Aggregate clicks and impressions for country-specific ranking in Merino. */
country_aggregates AS (
  SELECT
    corpus_item_id,
    normalized_country_code AS region,
    impression_count,
    click_count,
    report_count
  FROM
    aggregated_events
  WHERE
    -- Gather country (a.k.a. region) specific engagement for all countries that share a feed.
    -- https://mozilla-hub.atlassian.net/wiki/x/JY3LB
    normalized_country_code IN ('US', 'CA', 'DE', 'CH', 'AT', 'GB', 'IE')
),
/* Combine the "global" (no region) with the "regional" breakdown. */
combined_results AS (
  SELECT
    *
  FROM
    global_aggregates
  UNION ALL
  SELECT
    *
  FROM
    country_aggregates
)
SELECT
  *
FROM
  combined_results
ORDER BY
  impression_count DESC
LIMIT
  -- This LIMIT was derived from the 4 MB payload size cap in Merino, the observed average
  -- record size of ~113 bytes, and recall measurements. At ~20k rows the JSON blob stays
  -- well below 4 MB while still retaining >99.9% of fresh impressions globally. Smaller
  -- countries with lower traffic, like BE, still maintain an acceptable recall of about 97%.
  20000;
