CREATE OR REPLACE TABLE
  `mozilla-public-data`.telemetry_derived.client_probe_counts
AS
SELECT
  metric,
  ARRAY_AGG(DISTINCT(process)) AS processes,
FROM
  `moz-fx-data-shared-prod.telemetry.client_probe_counts`
GROUP BY
  metric
