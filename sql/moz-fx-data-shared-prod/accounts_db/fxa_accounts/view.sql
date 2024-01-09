CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.accounts_db.fxa_accounts`
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
  `moz-fx-data-shared-prod.accounts_db_external.fxa_accounts_v1`
