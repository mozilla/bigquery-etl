CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_ios.retention`
AS
SELECT
  *,
  -- not sure what the definition of lifecycle_stage is, so this is a placeholder
  -- for now just to show where we could place it.
  CASE
    WHEN EXTRACT(YEAR FROM first_seen_date) = 2024
      THEN "NEW"
    WHEN EXTRACT(YEAR FROM first_seen_date) < 2024
      THEN "EXISTING"
    ELSE "UKNOWN"
  END AS lifecycle_stage,
FROM
  `moz-fx-data-shared-prod.firefox_ios_derived.retention_v1`
