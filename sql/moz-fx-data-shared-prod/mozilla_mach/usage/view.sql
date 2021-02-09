CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.mozilla_mach.usage`
AS SELECT
  * REPLACE(
    mozfun.norm.metadata(metadata) AS metadata)
FROM
  `moz-fx-data-shared-prod.mozilla_mach_stable.usage_v1`
