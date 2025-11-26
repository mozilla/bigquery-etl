-- Monitor daily BigQuery LOAD job success/failure counts for specific tables
-- Purpose: Alert when load job failures exceed BigQuery change table limit (1400/day)
-- Tracks: prod_articles.zyte_cache and prod_rss_news.rss_feed_items
WITH base AS (
  SELECT
    DATE(creation_time) AS date,
    destination_table.dataset_id AS dataset,
    destination_table.table_id AS table_id,
    error_result IS NULL AS is_success
  FROM
    `moz-fx-data-shared-prod.region-US.INFORMATION_SCHEMA.JOBS_BY_PROJECT`
  WHERE
    job_type = 'LOAD'
    AND DATE(creation_time) = @submission_date
    AND destination_table IS NOT NULL
    AND (
      (destination_table.dataset_id = 'prod_articles' AND destination_table.table_id = 'zyte_cache')
      OR (
        destination_table.dataset_id = 'prod_rss_news'
        AND destination_table.table_id = 'rss_feed_items'
      )
    )
)
SELECT
  @submission_date AS submission_date,
  COUNTIF(
    dataset = 'prod_articles'
    AND table_id = 'zyte_cache'
    AND is_success
  ) AS zyte_cache_success,
  COUNTIF(
    dataset = 'prod_articles'
    AND table_id = 'zyte_cache'
    AND NOT is_success
  ) AS zyte_cache_failure,
  COUNTIF(
    dataset = 'prod_rss_news'
    AND table_id = 'rss_feed_items'
    AND is_success
  ) AS rss_feed_items_success,
  COUNTIF(
    dataset = 'prod_rss_news'
    AND table_id = 'rss_feed_items'
    AND NOT is_success
  ) AS rss_feed_items_failure
FROM
  base
