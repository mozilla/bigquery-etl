-- Generated via ./bqetl generate stable_views
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.messaging_system.personalization_experiment`
AS
SELECT
  * REPLACE (mozfun.norm.metadata(metadata) AS metadata),
  LOWER(IFNULL(metadata.isp.name, "")) = "browserstack" AS is_bot_generated,
FROM
  `moz-fx-data-shared-prod.messaging_system_stable.personalization_experiment_v1`
