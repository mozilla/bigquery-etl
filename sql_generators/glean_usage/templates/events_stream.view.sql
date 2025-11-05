{% from 'macros.sql' import event_extras_by_type_struct -%}
{{ header }}

CREATE OR REPLACE VIEW
  `{{ project_id }}.{{ events_stream_view }}`
AS
SELECT
  *,
  {% if extras_by_type %}
    {{ event_extras_by_type_struct(extras_by_type) }} AS extras
  {% endif %}
FROM
  `{{ project_id }}.{{ events_stream_table }}`
