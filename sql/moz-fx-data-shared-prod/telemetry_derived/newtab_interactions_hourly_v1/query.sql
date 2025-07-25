WITH
  -- LEGACY Query
legacy_deduplicated_pings AS (
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod.activity_stream_live.impression_stats_v1`
    -- live is fine for backfills within the past 30 days, otherwise use stable
    --`moz-fx-data-shared-prod.activity_stream_stable.impression_stats_v1`
  WHERE
    -- ensures no overlap with legacy_historical
    submission_timestamp >= '2024-12-01'
    AND DATE(submission_timestamp) = @submission_date
  QUALIFY
    ROW_NUMBER() OVER (
      PARTITION BY
        DATE(submission_timestamp),
        document_id
      ORDER BY
        submission_timestamp DESC
    ) = 1
),
legacy_impression_data AS (
  SELECT
    d.*
  FROM
    legacy_deduplicated_pings AS d
  WHERE
    loaded IS NULL -- don't include loaded ping
    -- include only data from Firefox > 121
    AND SAFE_CAST(SPLIT(version, '.')[0] AS int64) <= 120
    -- ensure data is valid/non-empty
    AND ARRAY_LENGTH(tiles) >= 1
    -- exclude custom Newtab page for Fx China
    AND (page IS NULL OR page != 'https://newtab.firefoxchina.cn/newtab/as/activity-stream.html')
),
-- this CTE allows us to filter out >2 clicks from a given client on the same tile within 1 second
legacy_flattened_impression_data AS (
  SELECT
    TIMESTAMP_TRUNC(submission_timestamp, SECOND) AS submission_second,
    impression_id AS client_id, -- client_id renamed to impression_id in GCP
    flattened_tiles.id AS tile_id,
    --the 3x1 layout has a bug where we need to use the position of each element in the tiles array instead of the actual pos field
    IFNULL(flattened_tiles.pos, alt_pos) AS position,
    SUM(
      CASE
        WHEN click IS NULL
          AND block IS NULL
          AND pocket IS NULL
          THEN 1
        ELSE 0
      END
    ) AS impressions,
    SUM(CASE WHEN click IS NOT NULL THEN 1 ELSE 0 END) AS clicks,
    SUM(CASE WHEN pocket IS NOT NULL THEN 1 ELSE 0 END) AS pocketed,
    SUM(CASE WHEN block IS NOT NULL THEN 1 ELSE 0 END) AS blocked
  FROM
    legacy_impression_data
  CROSS JOIN
    UNNEST(legacy_impression_data.tiles) AS flattened_tiles
    WITH OFFSET AS alt_pos
  GROUP BY
    submission_second,
    client_id,
    tile_id,
    position
),
legacy_summary AS (
  SELECT
    DATE(submission_second) AS submission_date,
    CAST(NULL AS STRING) AS recommendation_id,
    tile_id,
    position,
    CAST(NULL AS STRING) AS product,
    CAST(NULL AS STRING) AS placement,
    CAST(NULL AS STRING) AS os,
    CAST(NULL AS STRING) AS form_factor,
    SUM(impressions) AS impression_count,
    SUM(clicks) AS click_count,
    SUM(pocketed) AS save_count,
    SUM(blocked) AS dismiss_count
  FROM
    legacy_flattened_impression_data
  WHERE
    clicks < 3 -- filters out >2 clicks from a given client on the same tile within 1 second
  GROUP BY
    submission_date,
    tile_id,
    position,
    product,
    placement,
    os,
    form_factor
),
-- this table was copied from snowflake for historical data as the stable table
-- only goes back 180 days. it is pre-aggregated.
legacy_historical_summary AS (
  SELECT
    happened_at AS submission_date,
    CAST(NULL AS STRING) AS recommendation_id,
    tile_id AS ad_id,
    position,
    CAST(NULL AS STRING) AS product,
    CAST(NULL AS STRING) AS placement,
    CAST(NULL AS STRING) AS os,
    CAST(NULL AS STRING) AS form_factor,
    impression_count,
    click_count,
    save_count,
    dismiss_count
  FROM
    `moz-fx-data-shared-prod.telemetry_derived.newtab_interactions_historical_legacy_v1`
  WHERE
    -- ensures no overlap with legacy data
    happened_at < '2024-12-01'
    AND happened_at = @submission_date
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
    --`moz-fx-data-shared-prod.firefox_desktop_stable.newtab_v1` -- use for backfill
  WHERE
    DATE(submission_timestamp) = @submission_date
  QUALIFY
    ROW_NUMBER() OVER (
      PARTITION BY
        DATE(submission_timestamp),
        document_id
      ORDER BY
        submission_timestamp DESC
    ) = 1
),
glean_flattened_pocket_events AS (
  SELECT
    document_id,
    submission_timestamp,
    events.name AS event_name,
    mozfun.map.get_key(events.extra, 'recommendation_id') AS recommendation_id,
    mozfun.map.get_key(events.extra, 'tile_id') AS tile_id,
    mozfun.map.get_key(events.extra, 'position') AS position,
    COUNT(1) OVER (
      PARTITION BY
        client_info.client_id,
        DATE(submission_timestamp),
        events.name
    ) AS user_event_count
  FROM
    glean_deduplicated_pings,
    UNNEST(events) AS events
  WHERE
    events.category = 'pocket'
    AND events.name IN ('impression', 'click', 'save', 'dismiss')
    AND (
      -- keep only data with a non-null recommendation ID or tile ID
      mozfun.map.get_key(events.extra, 'recommendation_id') IS NOT NULL
      OR mozfun.map.get_key(events.extra, 'tile_id') IS NOT NULL
    )
    AND mozfun.map.get_key(events.extra, 'scheduled_corpus_item_id') IS NULL
    AND SAFE_CAST(SPLIT(client_info.app_display_version, '.')[0] AS int64) >= 121
),
glean_summary AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    recommendation_id,
    COALESCE(SAFE_CAST(tile_id AS int), -1) AS tile_id,
    COALESCE(SAFE_CAST(position AS int), -1) AS position,
    CAST(NULL AS STRING) AS product,
    CAST(NULL AS STRING) AS placement,
    CAST(NULL AS STRING) AS os,
    CAST(NULL AS STRING) AS form_factor,
    SUM(CASE WHEN event_name = 'impression' THEN 1 ELSE 0 END) AS impression_count,
    SUM(CASE WHEN event_name = 'click' THEN 1 ELSE 0 END) AS click_count,
    SUM(CASE WHEN event_name = 'save' THEN 1 ELSE 0 END) AS save_count,
    SUM(CASE WHEN event_name = 'dismiss' THEN 1 ELSE 0 END) AS dismiss_count
  FROM
    glean_flattened_pocket_events
  WHERE
    -- exclude suspicious activity
    NOT (user_event_count > 50 AND event_name = 'click')
  GROUP BY
    submission_date,
    recommendation_id,
    tile_id,
    position,
    product,
    placement,
    os,
    form_factor
),
--with the addition of the unified api, we are bringing in data from the ads backend
uapi_summary AS (
  SELECT
    DATE(submission_hour) AS submission_date,
    CAST(NULL AS STRING) AS recommendation_id,
    ad_id AS tile_id,
    position,
    product,
    placement,
    os,
    form_factor,
    SUM(
      CASE
        WHEN interaction_type = 'impression'
          THEN interaction_count
        ELSE 0
      END
    ) AS impression_count,
    SUM(CASE WHEN interaction_type = 'click' THEN interaction_count ELSE 0 END) AS click_count,
    0 AS save_count,
    0 AS dismiss_count,
  FROM
    `moz-fx-data-shared-prod.ads_derived.interaction_aggregates_uapi_hourly_v1`
  WHERE
    DATE(submission_hour) = @submission_date
  GROUP BY
    submission_date,
    ad_id,
    position,
    product,
    placement,
    os,
    form_factor
)
-- union legacy and glean telemetry
SELECT
  *
FROM
  legacy_summary
UNION ALL
SELECT
  *
FROM
  glean_summary
UNION ALL
SELECT
  *
FROM
  uapi_summary
UNION ALL
SELECT
  *
FROM
  legacy_historical_summary
