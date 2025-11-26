CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.accounts_backend.accounts`
AS
SELECT
  uid,
  emailVerified,
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
  `moz-fx-data-shared-prod.accounts_backend_external.accounts_v1`
