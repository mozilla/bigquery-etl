CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.experiment_search_aggregates_hourly`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.telemetry_derived.experiment_search_aggregates_hourly_v1`
