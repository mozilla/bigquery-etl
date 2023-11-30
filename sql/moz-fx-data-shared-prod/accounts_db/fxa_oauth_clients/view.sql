CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.accounts_db.fxa_oauth_clients`
AS
SELECT
  TO_HEX(id) AS id,
  name,
  createdAt,
FROM
  `moz-fx-data-shared-prod.accounts_db_external.fxa_oauth_clients_v1`
