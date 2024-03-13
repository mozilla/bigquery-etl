CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.accounts_backend.users_first_seen`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.accounts_backend_derived.users_first_seen_v1`
