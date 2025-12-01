SELECT
  DATE(m.submission_timestamp) AS submission_date,
  cpu.cpu_type AS TDP,
  COUNT(DISTINCT(client_info.client_id)) AS users
FROM
  `moz-fx-data-shared-prod.firefox_desktop.metrics` AS m
JOIN
  `moz-fx-data-shared-prod.telemetry_derived.fx_health_ind_fqueze_cpu_info_v1` AS cpu
  ON cpu.cpu_name = m.metrics.string.system_cpu_name
  AND m.metrics.string.system_cpu_name IS NOT NULL
WHERE
  DATE(m.submission_timestamp) = @submission_date
GROUP BY
  DATE(m.submission_timestamp),
  cpu.cpu_type
