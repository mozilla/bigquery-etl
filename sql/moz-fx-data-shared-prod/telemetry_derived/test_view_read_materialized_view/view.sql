CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry_derived.test_view_read_materialized_view`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.telemetry_derived.test_materialized_view`
