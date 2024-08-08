EXPORT DATA
OPTIONS
  (
    uri = 'gs://moz-fx-data-prod-bigquery-etl/merino-test/name*.json',
    format = 'JSON',
    overwrite = TRUE
  )
AS
  --replicates logic of current Prefect query using Glean data and scheduled_corpus_item_id
  --https://github.com/Pocket/data-flows/blob/main-v2/data-products/src/firefox_newtab/sql/glean_firefox_new_tab_impressions_hourly/glean_firefox_new_tab_daily_engagement_by_tile_id_position_country_locale/data.sql
WITH deduplicated_pings AS (
  SELECT
    submission_timestamp,
    document_id,
    normalized_country_code,
    client_info,
    events,
    metrics.string.newtab_locale AS locale
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_live.newtab_v1`
  WHERE
    submission_timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 MINUTE)
  QUALIFY
    ROW_NUMBER() OVER (
      PARTITION BY
        DATE(submission_timestamp),
        document_id
      ORDER BY
        submission_timestamp DESC
    ) = 1
),
flattened_pocket_events AS (
  SELECT
    document_id,
    submission_timestamp,
    e.name AS event_name,
    mozfun.map.get_key(e.extra, 'scheduled_corpus_item_id') AS scheduled_corpus_item_id,
    mozfun.map.get_key(e.extra, 'position') AS position,
    locale,
    normalized_country_code AS country,
    COUNT(1) OVER (PARTITION BY document_id, e.name) AS user_event_count
  FROM
    deduplicated_pings,
    UNNEST(events) AS e
    --filter to Pocket events
  WHERE
    e.category = 'pocket'
    AND e.name IN ('impression', 'click', 'save', 'dismiss')
    --keep only data with a non-null scheduled corpus item ID
    AND (
      mozfun.map.get_key(e.extra, 'scheduled_corpus_item_id') IS NOT NULL
      --include only data from Firefox 121+
      AND SAFE_CAST(SPLIT(client_info.app_display_version, '.')[0] AS int64) >= 121
    )
)
SELECT
  DATE(submission_timestamp) AS happened_at,
  scheduled_corpus_item_id,
  COALESCE(SAFE_CAST(position AS int), -1) AS position,
  'CARDGRID' AS SOURCE,
  locale,
  country,
  SUM(CASE WHEN event_name = 'impression' THEN 1 ELSE 0 END) AS impression_count,
  SUM(CASE WHEN event_name = 'click' THEN 1 ELSE 0 END) AS click_count,
  SUM(CASE WHEN event_name = 'save' THEN 1 ELSE 0 END) AS save_count,
  SUM(CASE WHEN event_name = 'dismiss' THEN 1 ELSE 0 END) AS dismiss_count
FROM
  flattened_pocket_events
WHERE
  NOT (user_event_count > 50 AND event_name = 'click')
GROUP BY
  1,
  2,
  3,
  4,
  5,
  6
LIMIT
  100;
