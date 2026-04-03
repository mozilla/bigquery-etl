WITH params AS (
  SELECT
    DATE(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY) - INTERVAL 1 DAY) AS end_date,
    DATE(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY) - INTERVAL 7 DAY) AS start_date,
    DATE(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY) - INTERVAL 8 DAY) AS start_date_items
),
date_span AS (
  SELECT
    DAY
  FROM
    params,
    UNNEST(GENERATE_DATE_ARRAY(start_date, end_date)) AS day
),
target_regions AS (
  SELECT
    region
  FROM
    UNNEST(['US', 'CA', 'DE', 'CH', 'AT', 'GB', 'IE']) AS region
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
    submission_date,
    corpus_item_id,
    country AS region,
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
filtered_base AS (
  SELECT
    b.submission_date,
    b.corpus_item_id,
    b.region,
    b.impression_count,
    b.click_count
  FROM
    base b
  INNER JOIN
    corpus_items c
    ON b.corpus_item_id = c.corpus_item_id
),
aggregated_events AS (
  SELECT
    corpus_item_id,
    region,
    SUM(impression_count) AS impression_count,
    SUM(click_count) AS click_count
  FROM
    filtered_base
  GROUP BY
    corpus_item_id,
    region
),
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
    impression_count > 2000
),
per_region_impressions_per_item AS (
  SELECT
    region,
    ROUND(AVG(impression_count)) AS impressions_per_item
  FROM
    aggregated_events
  GROUP BY
    region
),
ranked_per_region AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY region ORDER BY click_count DESC) AS rank
  FROM
    per_region_ctr
),
per_region_stats AS (
  SELECT
    region,
    AVG(CASE WHEN rank <= 10 THEN ctr END) AS average_ctr_top10_items,
    AVG(CASE WHEN rank <= 2 THEN ctr END) AS average_ctr_top2_items
  FROM
    ranked_per_region
  GROUP BY
    region
),
daily_region_totals AS (
  SELECT
    submission_date,
    region,
    SUM(impression_count) AS total_impressions
  FROM
    filtered_base
  GROUP BY
    submission_date,
    region
),
per_region_total_impressions_per_day AS (
  SELECT
    tr.region,
    ROUND(AVG(COALESCE(drt.total_impressions, 0))) AS total_impressions_per_day
  FROM
    target_regions tr
  CROSS JOIN
    date_span ds
  LEFT JOIN
    daily_region_totals drt
    ON drt.region = tr.region
    AND drt.submission_date = ds.day
  GROUP BY
    tr.region
),
per_region_final AS (
  SELECT
    s.region,
    s.average_ctr_top10_items,
    s.average_ctr_top2_items,
    i.impressions_per_item,
    d.total_impressions_per_day
  FROM
    per_region_stats s
  JOIN
    per_region_impressions_per_item i
    USING (region)
  JOIN
    per_region_total_impressions_per_day d
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
    ROUND(AVG(impression_count)) AS impressions_per_item
  FROM
    aggregated_events_global
),
ranked_global AS (
  SELECT
    *,
    ROW_NUMBER() OVER (ORDER BY click_count DESC) AS rank
  FROM
    per_global_ctr
),
global_stats AS (
  SELECT
    AVG(CASE WHEN rank <= 10 THEN ctr END) AS average_ctr_top10_items,
    AVG(CASE WHEN rank <= 2 THEN ctr END) AS average_ctr_top2_items
  FROM
    ranked_global
),
daily_global_totals AS (
  SELECT
    submission_date,
    SUM(impression_count) AS total_impressions
  FROM
    filtered_base
  GROUP BY
    submission_date
),
-- Combine global stats with global impressions_per_item
global_total_impressions_per_day AS (
  SELECT
    ROUND(AVG(COALESCE(dgt.total_impressions, 0))) AS total_impressions_per_day
  FROM
    date_span ds
  LEFT JOIN
    daily_global_totals dgt
    ON dgt.submission_date = ds.day
),
global_final AS (
  SELECT
    NULL AS region,
    gs.average_ctr_top10_items,
    gs.average_ctr_top2_items,
    gip.impressions_per_item,
    gtipd.total_impressions_per_day
  FROM
    global_stats gs
  CROSS JOIN
    global_impressions_per_item gip
  CROSS JOIN
    global_total_impressions_per_day gtipd
)
SELECT
  region,
  average_ctr_top10_items,
  average_ctr_top2_items,
  impressions_per_item,
  total_impressions_per_day
FROM
  per_region_final
WHERE
  region IN ('US', 'CA', 'DE', 'CH', 'AT', 'GB', 'IE')
UNION ALL
SELECT
  region,
  average_ctr_top10_items,
  average_ctr_top2_items,
  impressions_per_item,
  total_impressions_per_day
FROM
  global_final
ORDER BY
  impressions_per_item DESC;
