CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.fenix.retention`
AS
SELECT
  *,
  CASE
    WHEN first_seen_date = metric_date
      THEN 'new_client'
    -- TODO: Should we rename "repeat_client" to something different to avoid a mix up
    -- between this field and the repeat_clients field?
    WHEN DATE_DIFF(metric_date, first_seen_date, DAY)
      BETWEEN 1
      AND 27
      THEN 'repeat_client'
    WHEN DATE_DIFF(metric_date, first_seen_date, DAY) >= 28
      THEN 'existing_client'
    ELSE 'Unknown'
  END AS lifecycle_stage
FROM
  `moz-fx-data-shared-prod.fenix_derived.retention_v1`
