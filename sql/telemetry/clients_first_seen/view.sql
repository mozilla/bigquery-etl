CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.clients_first_seen`
AS
SELECT
  *
FROM
  `moz-fx-data-derived-datasets.telemetry_derived.clients_first_seen_v1`
