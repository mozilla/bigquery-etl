CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.client_probe_processes`
AS
SELECT
  metric,
  os,
  channel,
  ARRAY_AGG(DISTINCT(process)) AS processes,
FROM
  `moz-fx-data-shared-prod.telemetry_derived.client_probe_counts_v1`
GROUP BY
  metric,
  os,
  channel
ORDER BY
  metric,
  os
