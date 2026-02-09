CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.rust_component_metrics.run_maintenance_vacuum_time`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.rust_component_derived.run_maintenance_vacuum_time_v1`
