CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.event_aggregates`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.telemetry_derived.event_aggregates_v1`
