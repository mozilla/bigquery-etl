CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.rust_components.ingest_time`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.rust_components_derived.ingest_time_v1`
