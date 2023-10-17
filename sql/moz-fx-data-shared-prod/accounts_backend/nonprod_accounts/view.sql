CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.accounts_backend.nonprod_accounts`
AS
SELECT
  uid,
  verifierVersion,
  verifierSetAt,
  createdAt,
  locale,
  lockedAt,
  profileChangedAt,
  keysChangedAt,
  ecosystemAnonId,
  disabledAt,
  metricsOptOutAt,
FROM
  `moz-fx-data-shared-prod.accounts_backend_external.nonprod_accounts_v1`
