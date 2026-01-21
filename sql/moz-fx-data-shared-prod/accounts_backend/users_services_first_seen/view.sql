CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.accounts_backend.users_services_first_seen`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.accounts_backend_derived.users_services_first_seen_v1`
