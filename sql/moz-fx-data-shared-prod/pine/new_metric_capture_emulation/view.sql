-- Generated via ./bqetl generate stable_views
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.pine.new_metric_capture_emulation`
AS
SELECT
  * REPLACE (
    mozfun.norm.metadata(metadata) AS metadata,
    mozfun.norm.glean_ping_info(ping_info) AS ping_info
  )
FROM
  `moz-fx-data-shared-prod.pine_stable.new_metric_capture_emulation_v1`
