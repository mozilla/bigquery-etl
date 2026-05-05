CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.ads.ppa_measurements_limited`
AS
SELECT
  collection_time,
  placement_id,
  ad_id,
  conversion_key,
  task_size,
  task_id,
  task_index,
  conversion_count,
FROM
  `moz-fx-ads-prod.ppa.measurements`
