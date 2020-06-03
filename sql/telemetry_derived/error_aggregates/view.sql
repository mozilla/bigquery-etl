CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry_derived.error_aggregates` AS (
    SELECT
      *
    FROM
      `moz-fx-data-shared-prod.telemetry_derived.error_aggregates_v1`
  )
