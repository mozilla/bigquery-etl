--- User-facing view for all mobile apps. Generated via sql_generators.active_users.
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.active_users_aggregates_mobile`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.fenix.active_users_aggregates`
UNION ALL
SELECT
  *
FROM
  `moz-fx-data-shared-prod.firefox_ios.active_users_aggregates`
UNION ALL
SELECT
  *
FROM
  `moz-fx-data-shared-prod.focus_ios.active_users_aggregates`
UNION ALL
SELECT
  *
FROM
  `moz-fx-data-shared-prod.klar_ios.active_users_aggregates`
UNION ALL
SELECT
  *
FROM
  `moz-fx-data-shared-prod.focus_android.active_users_aggregates`
