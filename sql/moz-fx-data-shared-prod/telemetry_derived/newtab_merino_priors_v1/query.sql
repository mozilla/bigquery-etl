WITH
-- Define common parameters
params AS (
  SELECT
    TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY) AS end_timestamp,
    TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY) - INTERVAL 7 DAY AS start_timestamp
),
newtab_events AS (
  SELECT
    submission_timestamp,
    normalized_country_code AS region,
    event.name AS event_name,
    SAFE_CAST(
      mozfun.map.get_key(event.extra, 'scheduled_corpus_item_id') AS STRING
    ) AS scheduled_corpus_item_id,
    SAFE_CAST(mozfun.map.get_key(event.extra, 'recommended_at') AS INT64) AS recommended_at
  FROM
    `moz-fx-data-shared-prod.firefox_desktop.newtab`,
    UNNEST(events) AS event,
    params
  WHERE
    submission_timestamp >= params.start_timestamp
    AND submission_timestamp < params.end_timestamp
    AND event.category = 'pocket'
    AND event.name IN ('impression', 'click')
    AND mozfun.map.get_key(event.extra, 'scheduled_corpus_item_id') IS NOT NULL
    AND SAFE_CAST(mozfun.map.get_key(event.extra, 'recommended_at') AS INT64) IS NOT NULL
    AND SAFE_CAST(mozfun.map.get_key(event.extra, 'recommended_at') AS INT64)
    BETWEEN 946684800000
    AND UNIX_MILLIS(
      CURRENT_TIMESTAMP()
    )  -- This ensures recommended_at is between Jan 1, 2000, and the current time to remain within BQ limits for dates
),
-- Flatten events and filter relevant data
flattened_newtab_events AS (
  SELECT
    *
  FROM
    newtab_events,
    params
  WHERE
    TIMESTAMP_MILLIS(recommended_at) >= params.start_timestamp
    AND TIMESTAMP_MILLIS(recommended_at) < params.end_timestamp
),
-- Aggregate events by scheduled_corpus_item_id and region
aggregated_events AS (
  SELECT
    scheduled_corpus_item_id,
    region,
    SUM(IF(event_name = 'impression', 1, 0)) AS impression_count,
    SUM(IF(event_name = 'click', 1, 0)) AS click_count
  FROM
    flattened_newtab_events
  GROUP BY
    scheduled_corpus_item_id,
    region
),
-- Calculate CTR per scheduled_corpus_item_id and region
per_region_ctr AS (
  SELECT
    scheduled_corpus_item_id,
    region,
    SAFE_DIVIDE(click_count, impression_count) AS ctr,
    impression_count,
    click_count
  FROM
    aggregated_events
  WHERE
    impression_count > 0
),
-- Calculate average impressions per item per region and round to whole number
per_region_impressions_per_item AS (
  SELECT
    region,
    ROUND(AVG(impression_count)) AS impressions_per_item
  FROM
    aggregated_events
  GROUP BY
    region
),
-- Rank items by click_count per region
ranked_per_region AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY region ORDER BY click_count DESC) AS rank
  FROM
    per_region_ctr
),
-- Select top 2 items per region
top2_per_region AS (
  SELECT
    scheduled_corpus_item_id,
    region,
    ctr
  FROM
    ranked_per_region
  WHERE
    rank <= 2
),
-- Calculate average CTR of top 2 items per region
per_region_stats AS (
  SELECT
    region,
    AVG(ctr) AS average_ctr_top2_items
  FROM
    top2_per_region
  GROUP BY
    region
),
-- Combine per-region stats with impressions_per_item
per_region_stats_with_impressions AS (
  SELECT
    s.region,
    s.average_ctr_top2_items,
    i.impressions_per_item
  FROM
    per_region_stats s
  JOIN
    per_region_impressions_per_item i
    USING (region)
),
-- Aggregate events globally
aggregated_events_global AS (
  SELECT
    scheduled_corpus_item_id,
    SUM(impression_count) AS impression_count,
    SUM(click_count) AS click_count
  FROM
    aggregated_events
  GROUP BY
    scheduled_corpus_item_id
),
-- Calculate CTR per scheduled_corpus_item_id globally
per_global_ctr AS (
  SELECT
    scheduled_corpus_item_id,
    SAFE_DIVIDE(click_count, impression_count) AS ctr,
    impression_count,
    click_count
  FROM
    aggregated_events_global
  WHERE
    impression_count > 0
),
-- Calculate average impressions per item globally and round to whole number
global_impressions_per_item AS (
  SELECT
    CAST(NULL AS STRING) AS region,
    ROUND(AVG(impression_count)) AS impressions_per_item
  FROM
    aggregated_events_global
),
-- Rank items by click_count globally
ranked_global AS (
  SELECT
    *,
    ROW_NUMBER() OVER (ORDER BY click_count DESC) AS rank
  FROM
    per_global_ctr
),
-- Select top 2 items globally
top2_global AS (
  SELECT
    scheduled_corpus_item_id,
    ctr
  FROM
    ranked_global
  WHERE
    rank <= 2
),
-- Calculate average CTR of top 2 items globally
global_stats AS (
  SELECT
    CAST(NULL AS STRING) AS region,
    AVG(ctr) AS average_ctr_top2_items
  FROM
    top2_global
),
-- Combine global stats with impressions_per_item
global_stats_with_impressions AS (
  SELECT
    s.region,
    s.average_ctr_top2_items,
    i.impressions_per_item
  FROM
    global_stats s
  CROSS JOIN
    global_impressions_per_item i
)
-- Final output combining per-region and global statistics
SELECT
  region,
  average_ctr_top2_items,
  impressions_per_item
FROM
  per_region_stats_with_impressions
UNION ALL
SELECT
  region,
  average_ctr_top2_items,
  impressions_per_item
FROM
  global_stats_with_impressions
ORDER BY
  impressions_per_item DESC;
