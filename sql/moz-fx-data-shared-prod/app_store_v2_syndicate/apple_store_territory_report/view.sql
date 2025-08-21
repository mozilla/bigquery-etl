CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.monitoring.apple_store_territory_report`
AS
SELECT
  *
FROM
  `moz-fx-data-bq-fivetran.firefox_app_store_v2_apple_store.apple_store__territory_report`
