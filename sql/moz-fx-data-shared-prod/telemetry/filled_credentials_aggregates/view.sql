CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.filled_credentials_aggregates`
AS
SELECT
  submission_date,
  country,
  clients,
  fill_counts,
  "address_filled" AS credential_type
FROM
  `moz-fx-data-shared-prod.telemetry_derived.filled_address_clients_aggregates_v1`
UNION ALL
SELECT
  submission_date,
  country,
  clients,
  fill_counts,
  "credit_card_filled" AS credential_type
FROM
  `moz-fx-data-shared-prod.telemetry_derived.filled_creditcard_clients_aggregates_v1`
UNION ALL
SELECT
  submission_date,
  country,
  clients,
  fill_counts,
  "login_filled" AS credential_type
FROM
  `moz-fx-data-shared-prod.telemetry_derived.filled_login_clients_aggregates_v1`
