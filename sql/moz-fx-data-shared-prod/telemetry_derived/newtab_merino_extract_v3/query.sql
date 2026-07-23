WITH private_pings AS (
  SELECT
    submission_timestamp,
    document_id,
    events,
    mozfun.newtab.surface_id_country(
      metrics.string.newtab_content_surface_id,
      NULL,
      metrics.string.newtab_content_country
    ) AS normalized_country_code,
    NULLIF(metrics.string.newtab_content_experiment_branch, '') AS experiment_branch
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_live.newtab_content_v1`
  WHERE
    submission_timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
),
deduplicated_pings AS (
  SELECT
    *
  FROM
    private_pings
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
    experiment_branch,
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
    experiment_branch,
    corpus_item_id,
    position,
    format,
    section_position,
    SUM(CASE WHEN event_name = 'impression' THEN 1 ELSE 0 END) AS raw_impression_count,
    SUM(CASE WHEN event_name = 'click' THEN 1 ELSE 0 END) AS click_count,
    SUM(CASE WHEN event_name = 'report_content_submit' THEN 1 ELSE 0 END) AS report_count
  FROM
    flattened_newtab_events
  GROUP BY
    normalized_country_code,
    experiment_branch,
    corpus_item_id,
    position,
    format,
    section_position
),
propensity_weights AS (
  SELECT
    country,
    position,
    tile_format,
    weight
  FROM
    `moz-fx-data-shared-prod.telemetry_derived.newtab_merino_propensity_v2`
  WHERE
    layout = 'SECTION_GRID'
    AND section_position IS NULL
    AND snapshot_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)
  QUALIFY
    snapshot_date = MAX(snapshot_date) OVER ()
),
/* Separate and adjust section events */
section_events AS (
  SELECT
    rw.normalized_country_code,
    rw.experiment_branch,
    rw.corpus_item_id,
    rw.raw_impression_count,
    -- apply propensity scaling to impressions only
    -- prefer exact-format weights, then fall back to 'any' format weights
    rw.raw_impression_count / COALESCE(
      wt_country_exact.weight,
      wt_global_exact.weight,
      wt_country_any.weight,
      wt_global_any.weight,
      1.0
    ) AS adjusted_impression_count,
    rw.report_count,
    rw.click_count
  FROM
    raw_grouped_totals rw
  LEFT JOIN
    propensity_weights wt_country_exact
    ON wt_country_exact.country = rw.normalized_country_code
    AND SAFE_CAST(wt_country_exact.position AS INT64) = rw.position
    AND wt_country_exact.tile_format = rw.format
  LEFT JOIN
    propensity_weights wt_country_any
    ON wt_country_any.country = rw.normalized_country_code
    AND SAFE_CAST(wt_country_any.position AS INT64) = rw.position
    AND wt_country_any.tile_format = 'any'
  LEFT JOIN
    propensity_weights wt_global_exact
    ON wt_global_exact.country IS NULL
    AND SAFE_CAST(wt_global_exact.position AS INT64) = rw.position
    AND wt_global_exact.tile_format = rw.format
  LEFT JOIN
    propensity_weights wt_global_any
    ON wt_global_any.country IS NULL
    AND SAFE_CAST(wt_global_any.position AS INT64) = rw.position
    AND wt_global_any.tile_format = 'any'
  WHERE
    rw.section_position IS NOT NULL
),
/* Separate non-section (grid) type events */
non_section_events AS (
  SELECT
    normalized_country_code,
    experiment_branch,
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
    fe.experiment_branch,
    SAFE_CAST(SUM(adjusted_impression_count) AS INT64) AS impression_count,
    SUM(click_count) AS click_count,
    SUM(report_count) AS report_count
  FROM
    combined_events fe
  GROUP BY
    1,
    2,
    3
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
    SUM(impression_count) AS impression_count,
    SUM(click_count) AS click_count,
    SUM(report_count) AS report_count
  FROM
    aggregated_events
  WHERE
    -- Gather country (a.k.a. region) specific engagement for all countries that share a feed.
    -- https://mozilla-hub.atlassian.net/wiki/x/JY3LB
    normalized_country_code IN (
      'US',
      'CA',
      'DE',
      'CH',
      'AT',
      'GB',
      'IE',
      'BE',
      'PL',
      'FR',
      'ES',
      'IT'
    )
  GROUP BY
    corpus_item_id,
    region
),
/* Add de_DE experiment-specific rows using the existing region field. */
experiment_region_aggregates AS (
  SELECT
    corpus_item_id,
    CONCAT(normalized_country_code, '-', experiment_branch) AS region,
    SUM(impression_count) AS impression_count,
    SUM(click_count) AS click_count,
    SUM(report_count) AS report_count
  FROM
    aggregated_events
  WHERE
    experiment_branch IS NOT NULL
    AND normalized_country_code = 'DE'
  GROUP BY
    corpus_item_id,
    region
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
  UNION ALL
  SELECT
    *
  FROM
    experiment_region_aggregates
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
