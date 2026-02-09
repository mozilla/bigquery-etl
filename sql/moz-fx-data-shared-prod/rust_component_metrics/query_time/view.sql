CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.rust_component_metrics.query_time`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.rust_component_derived.query_time_v1`
