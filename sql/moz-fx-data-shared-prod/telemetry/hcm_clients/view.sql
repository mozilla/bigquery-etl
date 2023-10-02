CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.hcm_clients_by_channel`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.telemetry_derived.hcm_clients_by_channel_v1`
