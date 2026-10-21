WITH latest_snapshot AS (
  SELECT
    *
  FROM
    `moz-fx-data-marketing-prod.google_ads_dts_mmc.p_ads_asset_lookup_4813401401`
  WHERE
    _PARTITIONDATE = (
      SELECT
        MAX(_PARTITIONDATE)
      FROM
        `moz-fx-data-marketing-prod.google_ads_dts_mmc.p_ads_asset_lookup_4813401401`
    )
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
FROM
  latest_snapshot
