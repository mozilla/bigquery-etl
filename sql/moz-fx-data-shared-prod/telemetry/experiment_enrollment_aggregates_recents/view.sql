CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.experiment_enrollment_aggregates_recents`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.telemetry_derived.experiment_enrollment_aggregates_recents_v1`
