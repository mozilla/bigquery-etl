CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.addon_names`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.telemetry_derived.addon_names_v1`
