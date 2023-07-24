CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod`.telemetry_derived.client_probe_counts
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod`.telemetry.client_probe_counts
