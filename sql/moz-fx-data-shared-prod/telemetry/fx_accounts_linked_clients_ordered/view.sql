CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.fx_accounts_linked_clients_ordered`
AS
SELECT
  client_id,
  linked_client_id,
  linkage_first_seen_date,
  linkage_last_seen_date,
  client_id_first_seen_date,
  linked_client_id_first_seen_date,
  client_id_last_seen_date,
  linked_client_id_last_seen_date
FROM
  `moz-fx-data-shared-prod.telemetry_derived.fx_accounts_linked_clients_ordered_v1`
