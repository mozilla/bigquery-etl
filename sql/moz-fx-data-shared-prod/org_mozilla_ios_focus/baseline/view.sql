CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.org_mozilla_ios_focus.baseline`
AS SELECT
  * REPLACE(
    mozfun.norm.metadata(metadata) AS metadata)
FROM
  `moz-fx-data-shared-prod.org_mozilla_ios_focus_stable.baseline_v1`
