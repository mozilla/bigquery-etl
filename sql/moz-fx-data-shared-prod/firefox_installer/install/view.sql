-- Generated via ./bqetl generate stable_views
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_installer.install`
AS
SELECT
  * REPLACE (mozfun.norm.metadata(metadata) AS metadata)
FROM
  `moz-fx-data-shared-prod.firefox_installer_stable.install_v1`
