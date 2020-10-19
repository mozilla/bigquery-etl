CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.nondesktop_clients_last_seen`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.telemetry.nondesktop_clients_last_seen_v1`
