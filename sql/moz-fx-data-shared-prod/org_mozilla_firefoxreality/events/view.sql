CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.org_mozilla_firefoxreality.events`
AS SELECT
  * REPLACE(
    mozfun.norm.metadata(metadata) AS metadata)
FROM
  `moz-fx-data-shared-prod.org_mozilla_firefoxreality_stable.events_v1`
