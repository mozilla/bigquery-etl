CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.tmp.retention`
  -- `moz-fx-data-shared-prod.firefox_ios.retention`
AS
SELECT
  *,
  CASE
    WHEN EXTRACT(YEAR FROM metric_date) = 2024
      THEN "NEW"
    WHEN EXTRACT(YEAR FROM metric_date) < 2024
      THEN "EXISTING"
  ELSE
    "UKNOWN"
  END AS lifecycle_stage,
FROM
  `moz-fx-data-shared-prod.tmp.retention_v1_ios`
  -- `moz-fx-data-shared-prod.firefox_ios_derived.retention_v1`
