CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.install_aggregates`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.telemetry_derived.install_aggregates_v1`
