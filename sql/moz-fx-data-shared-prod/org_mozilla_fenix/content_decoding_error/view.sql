-- Generated via ./bqetl generate stable_views
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.org_mozilla_fenix.content_decoding_error`
AS
SELECT
  * REPLACE (mozfun.norm.metadata(metadata) AS metadata),
  LOWER(IFNULL(metadata.isp.name, "")) = "browserstack" AS is_bot_generated,
FROM
  `moz-fx-data-shared-prod.org_mozilla_fenix_stable.content_decoding_error_v1`
