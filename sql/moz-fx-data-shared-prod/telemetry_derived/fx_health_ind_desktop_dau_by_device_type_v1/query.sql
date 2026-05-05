SELECT
  DATE(t.submission_timestamp) AS submission_date,
  cpu.cpu_type AS TDP,
  100 * COUNT(DISTINCT(client_id)) AS users
FROM
  `moz-fx-data-shared-prod.telemetry.main_1pct` AS t
JOIN
  `moz-fx-data-shared-prod.telemetry_derived.fx_health_ind_fqueze_cpu_info_v1` AS cpu
  ON cpu.cpu_name = environment.system.cpu.name
  AND environment.system.cpu.name IS NOT NULL
WHERE
  DATE(t.submission_timestamp) = @submission_date
GROUP BY
  DATE(t.submission_timestamp),
  cpu.cpu_type
