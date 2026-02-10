CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.rust_components.db_size_after_maintenance`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.rust_components_derived.db_size_after_maintenance_v1`
