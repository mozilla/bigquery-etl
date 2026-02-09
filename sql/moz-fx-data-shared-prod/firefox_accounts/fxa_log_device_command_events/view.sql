CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_accounts.fxa_log_device_command_events`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.firefox_accounts_derived.fxa_log_device_command_events_v1`
UNION ALL
SELECT
  *
FROM
  `moz-fx-data-shared-prod.firefox_accounts_derived.fxa_log_device_command_events_v2`
