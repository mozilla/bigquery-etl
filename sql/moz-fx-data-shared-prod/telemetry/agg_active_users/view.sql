CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.agg_active_users`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.telemetry_derived.agg_active_users_v1`
