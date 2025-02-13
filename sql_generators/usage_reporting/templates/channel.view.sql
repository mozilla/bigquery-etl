-- {{ header }}
CREATE OR REPLACE VIEW
  `{{ project_id }}.{{ channel_dataset }}.{{ view_name }}`
AS
SELECT
  {% if app_name == "firefox_desktop" %}
  "{{ channel_dataset }}" AS normalized_app_id,
  app_channel AS normalized_channel,
  {% endif %}
  *,
FROM
  `{{ project_id }}.{{ channel_dataset }}_derived.{{ table_name }}`
