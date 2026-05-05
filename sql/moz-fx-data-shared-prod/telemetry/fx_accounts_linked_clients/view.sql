CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.fx_accounts_linked_clients`
AS
SELECT
  client_id,
  linked_client_id,
  linkage_first_seen_date,
  linkage_last_seen_date
FROM
  `moz-fx-data-shared-prod.telemetry_derived.fx_accounts_linked_clients_v1`
