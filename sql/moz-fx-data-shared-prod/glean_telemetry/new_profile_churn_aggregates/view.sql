CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.glean_telemetry.new_profile_churn_aggregates`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.glean_telemetry_derived.new_profile_churn_aggregates_v1`
