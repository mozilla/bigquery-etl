WITH
  -- LEGACY Query
  legacy_deduplicated_pings AS (
    SELECT
      *   
    FROM
      `moz-fx-data-shared-prod.activity_stream_live.impression_stats_v1`   
    WHERE DATE(submission_timestamp) = @submission_date
      QUALIFY ROW_NUMBER() OVER (PARTITION BY DATE(submission_timestamp), document_id ORDER BY submission_timestamp DESC) = 1
  ),
  legacy_impression_data AS (
    SELECT
      d.*
    FROM
      legacy_deduplicated_pings AS d
    WHERE
      loaded IS NULL
      AND SAFE_CAST(SPLIT(version, '.')[0] AS int64) <= 120
      AND ARRAY_LENGTH(tiles) >= 1
      AND (page IS NULL OR page != 'https://newtab.firefoxchina.cn/newtab/as/activity-stream.html')
  ),
  legacy_flattened_impression_data AS (
    SELECT
      UNIX_SECONDS(submission_timestamp) AS submission_timestamp,
      impression_id AS client_id,
      flattened_tiles.id AS tile_id,
      IFNULL(flattened_tiles.pos, alt_pos) AS position,
      SUM(CASE WHEN click IS NULL AND block IS NULL AND pocket IS NULL THEN 1 ELSE 0 END) AS impressions,
      SUM(CASE WHEN click IS NOT NULL THEN 1 ELSE 0 END) AS clicks,
      SUM(CASE WHEN pocket IS NOT NULL THEN 1 ELSE 0 END) AS pocketed,
      SUM(CASE WHEN block IS NOT NULL THEN 1 ELSE 0 END) AS blocked
    FROM
      legacy_impression_data
    CROSS JOIN
      UNNEST(legacy_impression_data.tiles) AS flattened_tiles
      WITH OFFSET AS alt_pos
    GROUP BY
      submission_timestamp, client_id, tile_id, position
  ),
  legacy_summary AS (
    SELECT
      DATE(TIMESTAMP_SECONDS(submission_timestamp)) AS happened_at,
      CAST(NULL AS STRING) AS recommendation_id,
      tile_id,
      position,
      SUM(impressions) AS impression_count,
      SUM(clicks) AS click_count,
      SUM(pocketed) AS save_count,
      SUM(blocked) AS dismiss_count
    FROM
      legacy_flattened_impression_data
    WHERE
      clicks < 3
    GROUP BY
      happened_at, tile_id, position
  ),

  -- GLEAN Query
  glean_deduplicated_pings AS (
    SELECT
      submission_timestamp,
      document_id,
      normalized_country_code,
      client_info,
      events
    FROM
      `moz-fx-data-shared-prod.firefox_desktop_live.newtab_v1`
    WHERE DATE(submission_timestamp) >= '2024-11-15'
    AND DATE(submission_timestamp) = @submission_date
      QUALIFY ROW_NUMBER() OVER (PARTITION BY DATE(submission_timestamp), document_id ORDER BY submission_timestamp DESC) = 1
  ),
  glean_flattened_pocket_events AS (
    SELECT
      document_id,
      submission_timestamp,
      events.name AS event_name,
      mozfun.map.get_key(events.extra, 'recommendation_id') AS recommendation_id,
      mozfun.map.get_key(events.extra, 'tile_id') AS tile_id,
      mozfun.map.get_key(events.extra, 'position') AS position,
      COUNT(1) OVER (PARTITION BY document_id, events.name) AS user_event_count
    FROM
      glean_deduplicated_pings,
      UNNEST(events) AS events
    WHERE
      events.category = 'pocket'
      AND events.name IN ('impression', 'click', 'save', 'dismiss')
      AND (mozfun.map.get_key(events.extra, 'recommendation_id') IS NOT NULL OR mozfun.map.get_key(events.extra, 'tile_id') IS NOT NULL)
      AND mozfun.map.get_key(events.extra, 'scheduled_corpus_item_id') IS NULL
      AND SAFE_CAST(SPLIT(client_info.app_display_version, '.')[0] AS int64) >= 121
  ),
  glean_summary AS (
    SELECT
      DATE(submission_timestamp) AS happened_at,
      recommendation_id,
      COALESCE(SAFE_CAST(tile_id AS int), -1) AS tile_id,
      COALESCE(SAFE_CAST(position AS int), -1) AS position,
      SUM(CASE WHEN event_name = 'impression' THEN 1 ELSE 0 END) AS impression_count,
      SUM(CASE WHEN event_name = 'click' THEN 1 ELSE 0 END) AS click_count,
      SUM(CASE WHEN event_name = 'save' THEN 1 ELSE 0 END) AS save_count,
      SUM(CASE WHEN event_name = 'dismiss' THEN 1 ELSE 0 END) AS dismiss_count
    FROM
      glean_flattened_pocket_events
    WHERE NOT (user_event_count > 50 AND event_name = 'click')
    GROUP BY
      happened_at, recommendation_id, tile_id, position
  )

-- Combined Query with Full Outer Join
SELECT
  COALESCE(l.happened_at, g.happened_at) AS happened_at,
  COALESCE(l.recommendation_id, g.recommendation_id) AS recommendation_id,
  COALESCE(l.tile_id, g.tile_id) AS tile_id,
  COALESCE(l.position, g.position) AS position,
  IFNULL(l.impression_count, 0) + IFNULL(g.impression_count, 0) AS total_impression_count,
  IFNULL(l.click_count, 0) + IFNULL(g.click_count, 0) AS total_click_count,
  IFNULL(l.save_count, 0) + IFNULL(g.save_count, 0) AS total_save_count,
  IFNULL(l.dismiss_count, 0) + IFNULL(g.dismiss_count, 0) AS total_dismiss_count
FROM
  legacy_summary AS l
FULL OUTER JOIN
  glean_summary AS g
ON
  l.happened_at = g.happened_at
  AND l.tile_id = g.tile_id
  AND l.position = g.position
ORDER BY
  happened_at, tile_id, position;
