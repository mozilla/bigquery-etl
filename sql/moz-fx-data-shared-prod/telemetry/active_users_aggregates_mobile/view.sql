--- User-facing view for all mobile apps. Generated via sql_generators.active_users_aggregates.
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.active_users_aggregates_mobile`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.fenix_derived.active_users_aggregates_v1`
UNION ALL
SELECT
  *
FROM
  `moz-fx-data-shared-prod.firefox_ios_derived.active_users_aggregates_v1`
UNION ALL
SELECT
  *
FROM
  `moz-fx-data-shared-prod.focus_ios_derived.active_users_aggregates_v1`
UNION ALL
SELECT
  *
FROM
  `moz-fx-data-shared-prod.focus_android_derived.active_users_aggregates_v1`
UNION ALL
SELECT
  *
FROM
  `moz-fx-data-shared-prod.klar_ios_derived.active_users_aggregates_v1`
