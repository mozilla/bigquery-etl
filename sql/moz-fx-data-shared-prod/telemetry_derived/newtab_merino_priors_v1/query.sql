WITH params AS (
  SELECT
    DATE(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY) - INTERVAL 1 DAY) AS end_date,
    DATE(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY) - INTERVAL 7 DAY) AS start_date,
    DATE(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY) - INTERVAL 8 DAY) AS start_date_items
),
corpus_items AS (
  SELECT DISTINCT
    corpus_item_id
  FROM
    `moz-fx-data-shared-prod.snowflake_migration_derived.corpus_items_current_v1`
  WHERE
    DATE(corpus_item_updated_at) > (SELECT start_date_items FROM params)
),
base AS (
  SELECT
    corpus_item_id,
    country AS region,    -- rename here
    impression_count,
    click_count
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_derived.newtab_content_items_daily_combined_v1`
  WHERE
    corpus_item_id IS NOT NULL
    AND country IS NOT NULL
    AND submission_date
    BETWEEN (SELECT start_date FROM params)
    AND (SELECT end_date FROM params)
),
  -- Keep only items that exist in corpus_items and aggregate by item/region
aggregated_events AS (
  SELECT
    b.corpus_item_id,
    b.region,
    SUM(b.impression_count) AS impression_count,
    SUM(b.click_count) AS click_count
  FROM
    base b
  INNER JOIN
    corpus_items c
    ON b.corpus_item_id = c.corpus_item_id
  GROUP BY
    b.corpus_item_id,
    b.region
),
  -- Calculate CTR per corpus_item_id and region
per_region_ctr AS (
  SELECT
    corpus_item_id,
    region,
    SAFE_DIVIDE(click_count, impression_count) AS ctr,
    impression_count,
    click_count
  FROM
    aggregated_events
  WHERE
    impression_count > 2000 -- 1/min_ctr protects against CTRs form low sample size
),
  -- Average impressions per item per region (rounded)
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
  -- Top 2 items per region
top2_per_region AS (
  SELECT
    corpus_item_id,
    region,
    ctr
  FROM
    ranked_per_region
  WHERE
    rank <= 2
),
  -- Average CTR of top 2 items per region
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
    corpus_item_id,
    SUM(impression_count) AS impression_count,
    SUM(click_count) AS click_count
  FROM
    aggregated_events
  GROUP BY
    corpus_item_id
),
  -- CTR per item globally
per_global_ctr AS (
  SELECT
    corpus_item_id,
    SAFE_DIVIDE(click_count, impression_count) AS ctr,
    impression_count,
    click_count
  FROM
    aggregated_events_global
  WHERE
    impression_count > 2000
),
  -- Avg impressions per item globally (rounded)
global_impressions_per_item AS (
  SELECT
    CAST(NULL AS STRING) AS region,
    ROUND(AVG(impression_count)) AS impressions_per_item
  FROM
    aggregated_events_global
),
  -- Rank items globally by click_count
ranked_global AS (
  SELECT
    *,
    ROW_NUMBER() OVER (ORDER BY click_count DESC) AS rank
  FROM
    per_global_ctr
),
  -- Top 2 items globally
top2_global AS (
  SELECT
    corpus_item_id,
    ctr
  FROM
    ranked_global
  WHERE
    rank <= 2
),
  -- Avg CTR of top 2 items globally
global_stats AS (
  SELECT
    CAST(NULL AS STRING) AS region,
    AVG(ctr) AS average_ctr_top2_items
  FROM
    top2_global
),
  -- Combine global stats with global impressions_per_item
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
