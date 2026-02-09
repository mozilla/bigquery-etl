CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.rust_component_metrics.db_size_after_maintenance`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.rust_component_derived.db_size_after_maintenance_v1`
