CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.rust_component_metrics.ingest_download_time`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.rust_component_derived.ingest_download_time_v1`
