CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_accounts.fxa_users_services_first_seen`
AS
SELECT
  * EXCEPT (first_service_flow),
  first_service_flow AS first_service_flow_id,
FROM
  `moz-fx-data-shared-prod.firefox_accounts_derived.fxa_users_services_first_seen_v2`
