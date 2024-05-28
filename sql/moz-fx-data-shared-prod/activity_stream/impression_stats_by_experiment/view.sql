CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.activity_stream.impression_stats_by_experiment`
AS
SELECT
  tile_id,
  IFNULL(spoc_tile_ids.type, 'curated') AS tile_type,
  stats.* EXCEPT (tile_id)
FROM
  `moz-fx-data-shared-prod.activity_stream_bi.impression_stats_by_experiment_v1` AS stats
LEFT JOIN
  `moz-fx-data-shared-prod.pocket.spoc_tile_ids` AS spoc_tile_ids
  USING (tile_id)
