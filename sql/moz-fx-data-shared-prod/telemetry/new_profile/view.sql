CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.new_profile`
AS SELECT
  * REPLACE(
    mozfun.norm.metadata(metadata) AS metadata)
FROM
  `moz-fx-data-shared-prod.telemetry_stable.new_profile_v4`
