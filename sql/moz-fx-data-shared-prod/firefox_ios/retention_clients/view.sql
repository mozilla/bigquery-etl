CREATE OR REPLACE VIEW
  -- `moz-fx-data-shared-prod.firefox_ios.retention_clients`
  `moz-fx-data-shared-prod.tmp.retention_clients`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.tmp.retention_clients_v1_ios`
  -- `moz-fx-data-shared-prod.firefox_ios_derived.retention_clients_v1`
