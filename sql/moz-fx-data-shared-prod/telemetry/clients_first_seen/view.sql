CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.clients_first_seen`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.telemetry_derived.clients_first_seen_v2`
