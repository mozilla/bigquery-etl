CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry_derived.install_v1`
AS
SELECT
  * REPLACE (udf_js.decode_uri_attribution(attribution) AS attribution)
FROM
  `moz-fx-data-shared-prod.firefox_installer.install`
