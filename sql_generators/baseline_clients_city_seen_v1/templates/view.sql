--- User-facing view. Generated via sql_generators.baseline_clients_city_seen.
CREATE OR REPLACE VIEW
  `{{ project_id }}.{{ app_name }}.baseline_clients_city_seen`
AS
SELECT
  *
FROM
  `{{ project_id }}.{{ app_name }}_derived.{{ table_name }}`
