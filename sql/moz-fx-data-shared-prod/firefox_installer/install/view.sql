-- Generated via ./bqetl generate stable_views
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_installer.install`
AS
SELECT
  * REPLACE (mozfun.norm.metadata(metadata) AS metadata),
  `moz-fx-data-shared-prod`.udf.funnel_derived_installs(
    silent,
    submission_timestamp,
    build_id,
    attribution,
    distribution_id
  ) AS funnel_derived,
  `moz-fx-data-shared-prod`.udf.distribution_model_installs(distribution_id) AS distribution_model,
  `moz-fx-data-shared-prod`.udf.partner_org_installs(distribution_id) AS partner_org,
  LOWER(IFNULL(metadata.isp.name, "")) = "browserstack" AS is_bot_generated,
FROM
  `moz-fx-data-shared-prod.firefox_installer_stable.install_v1`
