-- Generated via ./bqetl generate stable_views
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.org_mozilla_fenix.usage_reporting`
AS
SELECT
  * REPLACE (mozfun.norm.metadata(metadata) AS metadata),
FROM
  `moz-fx-data-shared-prod.org_mozilla_fenix_stable.usage_reporting_v1`