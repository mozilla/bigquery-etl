--- User-facing view. Generated via sql_generators.active_users.
CREATE OR REPLACE VIEW
  `{{ project_id }}.{{ app_name }}.urlbar_search_sessions`
AS
SELECT
  *
FROM
  `{{ project_id }}.{{ app_name }}_derived.urlbar_search_sessions_v1`
