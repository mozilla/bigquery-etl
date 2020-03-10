CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.activity_stream.impression_stats_flat`
AS
SELECT
  tile_id,
  tile_id_types.type AS tile_type,
  stats.* EXCEPT (tile_id)
FROM
  `moz-fx-data-shared-prod.activity_stream_bi.impression_stats_flat_v1` AS stats
LEFT JOIN
  `moz-fx-data-shared-prod.activity_stream.tile_id_types` AS tile_id_types
USING
  (tile_id)
