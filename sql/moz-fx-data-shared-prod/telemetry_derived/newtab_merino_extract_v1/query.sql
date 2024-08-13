WITH deduplicated_pings AS (
  SELECT
    submission_timestamp,
    document_id,
    events,
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_live.newtab_v1`
  WHERE
    submission_timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
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
    unnested_events.name AS event_name,
    mozfun.map.get_key(
      unnested_events.extra,
      'scheduled_corpus_item_id'
    ) AS scheduled_corpus_item_id,
    TIMESTAMP_MILLIS(CAST(mozfun.map.get_key(
    unnested_events.extra,
    'recommended_at'
  ) AS INT64)) AS recommended_at
  FROM
    deduplicated_pings,
    UNNEST(events) AS unnested_events
    --filter to Pocket events
  WHERE
    unnested_events.category = 'pocket'
    AND unnested_events.name IN ('impression', 'click')
    --keep only data with a non-null scheduled corpus item ID
    AND (mozfun.map.get_key(unnested_events.extra, 'scheduled_corpus_item_id') IS NOT NULL)
)
SELECT
  scheduled_corpus_item_id,
  SUM(CASE WHEN event_name = 'impression' THEN 1 ELSE 0 END) AS impression_count,
  SUM(CASE WHEN event_name = 'click' THEN 1 ELSE 0 END) AS click_count
FROM
  flattened_newtab_events
WHERE recommended_at > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
GROUP BY
  scheduled_corpus_item_id
