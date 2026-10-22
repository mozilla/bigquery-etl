-- Fails the query (and keeps the previous table) if no snapshot loaded in the
-- last 7 days. The constant lower bound also limits the partitions scanned.
WITH latest_snapshot_date AS (
  SELECT
    IFNULL(
      MAX(_PARTITIONDATE),
      ERROR('No asset_lookup snapshot in the last 7 days')
    ) AS snapshot_date
  FROM
    `moz-fx-data-marketing-prod.google_ads_dts_mmc.p_ads_asset_lookup_4813401401`
  WHERE
    _PARTITIONDATE >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
),
recent_snapshots AS (
  SELECT
    *,
    _PARTITIONDATE AS snapshot_date,
  FROM
    `moz-fx-data-marketing-prod.google_ads_dts_mmc.p_ads_asset_lookup_4813401401`
  WHERE
    _PARTITIONDATE >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
),
-- snapshot_date comes from latest_snapshot_date and is in the output, so
-- BigQuery must evaluate the ERROR() above even when no recent partition exists.
latest_snapshot AS (
  SELECT
    latest_snapshot_date.snapshot_date,
    recent_snapshots.* EXCEPT (snapshot_date),
  FROM
    latest_snapshot_date
  LEFT JOIN
    recent_snapshots
    USING (snapshot_date)
)
-- Naming convention: uac_<language>_rt<N>_<theme>_<aspect ratio>_<MM-YY>_<seq>...
SELECT
  asset_id,
  asset_name,
  asset_type,
  asset_image_asset_full_size_url AS image_url,
  asset_youtube_video_asset_youtube_video_id AS youtube_video_id,
  asset_youtube_video_asset_youtube_video_title AS youtube_video_title,
  asset_text_asset_text AS text,
  COALESCE(
    asset_image_asset_full_size_url,
    'https://i.ytimg.com/vi/' || asset_youtube_video_asset_youtube_video_id || '/hqdefault.jpg'
  ) AS thumbnail_url,
  REGEXP_EXTRACT(asset_name, r'^uac_([a-z]{2,3})_rt\d+_[^_]+_') AS language,
  SAFE_CAST(REGEXP_EXTRACT(asset_name, r'^uac_[a-z]{2,3}_rt(\d+)_[^_]+_') AS INT64) AS rt,
  REGEXP_EXTRACT(asset_name, r'^uac_[a-z]{2,3}_rt\d+_([^_]+)_') AS raw_theme,
  snapshot_date,
FROM
  latest_snapshot
