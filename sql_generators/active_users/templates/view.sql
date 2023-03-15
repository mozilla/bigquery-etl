--- User-facing view. Generated via sql_generators.active_users_aggregates.
CREATE OR REPLACE VIEW
  `{{ project_id }}.{{ app_name }}.active_users_aggregates`
AS
SELECT
  *,
  `mozfun.norm.os`(os) AS os_grouped
FROM
  `{{ project_id }}.{{ app_name }}_derived.active_users_aggregates_v1`
