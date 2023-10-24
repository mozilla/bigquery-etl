--- User-facing view. Generated via sql_generators.active_users.
CREATE OR REPLACE VIEW
  `{{ project_id }}.{{ app_name }}.review_checker_clients`
AS
SELECT
  *
FROM
  `{{ project_id }}.{{ app_name }}_derived.review_checker_clients_v1`
