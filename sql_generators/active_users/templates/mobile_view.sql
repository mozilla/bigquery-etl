--- User-facing view for all mobile apps. Generated via sql_generators.active_users_aggregates.
CREATE OR REPLACE VIEW
  `{{ project_id }}.{{ telemetry }}.active_users_aggregates_mobile`
AS
SELECT
  *
FROM
  `{{ project_id }}.fenix_derived.active_users_aggregates_v1`
UNION ALL
SELECT
  *
FROM
  `{{ project_id }}.firefox_ios_derived.active_users_aggregates_v1`
UNION ALL
SELECT
  *
FROM
  `{{ project_id }}.focus_ios_derived.active_users_aggregates_v1`
UNION ALL
SELECT
  *
FROM
  `{{ project_id }}.focus_android_derived.active_users_aggregates_v1`
UNION ALL
SELECT
  *
FROM
  `{{ project_id }}.klar_ios_derived.active_users_aggregates_v1`
