SELECT
  metric,
  ARRAY_AGG(DISTINCT(process)) AS processes,
FROM
  `moz-fx-data-shared-prod.telemetry.client_probe_counts`
GROUP BY
  metric
