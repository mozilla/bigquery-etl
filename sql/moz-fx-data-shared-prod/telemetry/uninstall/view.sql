-- Generated via ./bqetl generate stable_views
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.uninstall`
AS
SELECT
  * REPLACE (mozfun.norm.metadata(metadata) AS metadata)
FROM
  `moz-fx-data-shared-prod.telemetry_stable.uninstall_v4`