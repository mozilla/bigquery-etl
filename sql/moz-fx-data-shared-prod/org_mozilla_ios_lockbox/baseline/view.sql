CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.org_mozilla_ios_lockbox.baseline`
AS SELECT
  * REPLACE(
    mozfun.norm.metadata(metadata) AS metadata)
FROM
  `moz-fx-data-shared-prod.org_mozilla_ios_lockbox_stable.baseline_v1`
