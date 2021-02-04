SELECT
  -- truncate timestamp to seconds
  TIMESTAMP_TRUNC(submission_timestamp, SECOND) AS submission_timestamp,
  impression_id AS client_id, -- client_id renamed to impression_id in GCP
  flattened_tiles.id AS tile_id,
  addon_version,
  COUNT(loaded) AS loaded,
  COUNTIF(click IS NULL AND block IS NULL AND pocket IS NULL AND loaded IS NULL) AS impressions,
  COUNT(click) AS clicks,
  COUNT(block) AS blocked,
  COUNT(pocket) AS pocketed,
  -- the 3x1 layout has a bug where we need to use the position of each element
  -- in the tiles array instead of the actual pos field
  IFNULL(flattened_tiles.pos, alt_pos) AS position,
  page,
  source,
  locale,
  normalized_country_code AS country_code,
  version,
  user_prefs,
  release_channel,
  IFNULL(shield_id, 'n/a') AS shield_id,
  ANY_VALUE(experiments) AS experiments,
  sample_id
FROM
  activity_stream_stable.impression_stats_v1
CROSS JOIN
  UNNEST(tiles) AS flattened_tiles
  WITH OFFSET AS alt_pos
WHERE
  (@submission_date IS NULL OR @submission_date = DATE(submission_timestamp))
GROUP BY
  submission_timestamp,
  impression_id,
  tile_id,
  addon_version,
  position,
  page,
  source,
  locale,
  country_code,
  version,
  user_prefs,
  release_channel,
  shield_id,
  sample_id
