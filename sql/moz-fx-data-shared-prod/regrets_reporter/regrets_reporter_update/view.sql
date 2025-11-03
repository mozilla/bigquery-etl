-- Generated via ./bqetl generate stable_views
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.regrets_reporter.regrets_reporter_update`
AS
SELECT
  * REPLACE (mozfun.norm.metadata(metadata) AS metadata),
  LOWER(IFNULL(metadata.isp.name, "")) = "browserstack" AS is_bot_generated,
FROM
  `moz-fx-data-shared-prod.regrets_reporter_stable.regrets_reporter_update_v1`
