CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.active_users_rollup_shredder`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.telemetry_derived.active_users_rollup_shredder_v1`
