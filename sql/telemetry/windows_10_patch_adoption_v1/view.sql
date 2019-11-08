CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.windows_10_patch_adoption_v1` AS
SELECT
  CAST(ubr AS int64) numeric_windows_ubr,
  build_number,
  ubr label,
  SUM(count) frequency
FROM
  `moz-fx-data-shared-prod.telemetry.windows_10_aggregate_v1`
GROUP BY
  1,
  2,
  3
ORDER BY
  1 ASC
