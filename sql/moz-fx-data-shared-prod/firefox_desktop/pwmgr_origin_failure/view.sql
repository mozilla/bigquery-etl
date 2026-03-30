-- Generated via ./bqetl generate stable_views
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_desktop.pwmgr_origin_failure`
AS
SELECT
  * REPLACE (mozfun.norm.metadata(metadata) AS metadata),
  LOWER(IFNULL(metadata.isp.name, "")) = "browserstack" AS is_bot_generated,
FROM
  `moz-fx-data-shared-prod.firefox_desktop_stable.pwmgr_origin_failure_v1`
