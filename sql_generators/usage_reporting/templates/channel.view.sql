-- {{ header }}
CREATE OR REPLACE VIEW
  `{{ project_id }}.{{ channel_dataset }}.{{ view_name }}`
AS
SELECT
  *
FROM
  `{{ project_id }}.{{ channel_dataset }}_derived.{{ table_name }}`
