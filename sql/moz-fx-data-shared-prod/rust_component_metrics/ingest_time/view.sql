CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.rust_component_metrics.ingest_time`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.rust_component_derived.ingest_time_v1`
