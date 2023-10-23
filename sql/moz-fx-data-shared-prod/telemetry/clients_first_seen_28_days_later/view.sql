CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.clients_first_seen_28_days_later`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.telemetry_derived.clients_first_seen_28_days_later_v1`
