CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_ios.retention`
AS
SELECT
  *,
  CASE
    WHEN EXTRACT(metric_date, YEAR) = 2024
      THEN "NEW"
    WHEN EXTRACT(metric_date, YEAR) < 2024
      THEN "EXISTING"
  ELSE
    "UKNOWN"
  END AS lifecycle_stage,
FROM
  `moz-fx-data-shared-prod.firefox_ios_derived.retention_v1`
