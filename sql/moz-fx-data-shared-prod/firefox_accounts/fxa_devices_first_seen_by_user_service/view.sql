CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_accounts.fxa_devices_first_seen_by_user_service`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.firefox_accounts_derived.fxa_devices_first_seen_by_user_service_v1`
