-- This is an authorized view that allows us to read specific data from
-- a project owned by the Pocket team; if the definition here changes,
-- it may need to be manually redeployed by Data Ops.
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.activity_stream.tile_id_types`
AS
SELECT
  *
FROM
  `pocket-tiles.pocket_tiles_data.tile_id_types`
