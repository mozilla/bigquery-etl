CREATE OR REPLACE VIEW
  `moz-fx-glam-prod.glam_etl.latest_versions` AS (
    SELECT
      *
    FROM
      `moz-fx-data-shared-prod.telemetry_derived.latest_versions`
  )
