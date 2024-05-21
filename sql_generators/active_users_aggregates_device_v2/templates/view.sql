--- User-facing view for all mobile apps. Generated via sql_generators.active_users.
CREATE OR REPLACE VIEW
    `{{ project_id }}.{{ app_name }}.active_users_aggregates_device`
AS
SELECT
    *
FROM
    `{{ project_id }}.{{ app_name }}_derived.{{ table_name}}`
