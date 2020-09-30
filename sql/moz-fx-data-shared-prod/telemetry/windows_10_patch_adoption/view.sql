CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.windows_10_patch_adoption` AS
SELECT
  CAST(ubr AS int64) AS numeric_windows_ubr,
  build_number,
  ubr AS label,
  SUM(count) AS frequency
FROM
  `moz-fx-data-shared-prod.telemetry.windows_10_aggregate`
GROUP BY
  numeric_windows_ubr,
  build_number,
  label
ORDER BY
  numeric_windows_ubr ASC
