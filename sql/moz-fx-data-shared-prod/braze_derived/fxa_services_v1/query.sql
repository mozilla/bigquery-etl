-- Full per-user state of FxA ToS, flattened to
-- one row per (uid, service). Because the upstream accounts_db_external tables are
-- full-table mirrors, this is always the complete current set of a user's authorized services.
-- The downstream braze_external.changed_fxa_services_sync_v1 table picks out the rows
-- that are new since the last sync and appends only those.
SELECT
  a.uid AS uid,
  MIN(e.email) AS email,
  a.service AS service,
  MIN(a.firstAuthorizedTosAt) AS first_authorized_tos_at,
FROM
  `moz-fx-data-shared-prod.accounts_db_external.fxa_oauth_account_authorizations_v1` AS a
INNER JOIN
  `moz-fx-data-shared-prod.accounts_db_external.fxa_emails_v1` AS e
  ON a.uid = e.uid
  AND e.isPrimary
WHERE
  a.service IS NOT NULL
GROUP BY
  uid,
  service;
