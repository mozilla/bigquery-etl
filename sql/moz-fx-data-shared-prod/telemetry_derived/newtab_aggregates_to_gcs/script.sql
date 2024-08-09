EXPORT DATA OPTIONS(
    uri='gs://moz-fx-data-prod-bigquery-etl/merino-test/export_*.json',
    format='JSON',
    overwrite=true
) AS

WITH
  deduplicated_pings AS (
  SELECT
    submission_timestamp,
    document_id,
    events,
  FROM
      `moz-fx-data-shared-prod.firefox_desktop_live.newtab_v1`
  WHERE submission_timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY DATE(submission_timestamp),
      document_id
    ORDER BY
      submission_timestamp DESC) = 1 ),
  flattened_newtab_events AS (
  SELECT
    document_id,
    submission_timestamp,
    e.name AS event_name,
    mozfun.map.get_key(e.extra,
      'scheduled_corpus_item_id') AS scheduled_corpus_item_id,
    mozfun.map.get_key(e.extra,
      'position') AS position,
    COUNT(1) OVER (PARTITION BY document_id, e.name) AS user_event_count
  FROM
    deduplicated_pings,
    UNNEST(events) AS e
    --filter to Pocket events
  WHERE
    e.category = 'pocket'
    AND e.name IN ('impression',
      'click',
      'save',
      'dismiss')
    --keep only data with a non-null scheduled corpus item ID
    AND (mozfun.map.get_key(e.extra,
        'scheduled_corpus_item_id') IS NOT NULL
    ))
SELECT
  scheduled_corpus_item_id,
  SUM(CASE
      WHEN event_name = 'impression' THEN 1
      ELSE 0
  END
    ) AS impression_count,
  SUM(CASE
      WHEN event_name = 'click' THEN 1
      ELSE 0
  END
    ) AS click_count
FROM
  flattened_newtab_events
GROUP BY
  1;