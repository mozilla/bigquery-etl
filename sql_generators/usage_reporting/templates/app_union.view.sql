-- {{ header }}
CREATE OR REPLACE VIEW
  `{{ project_id }}.{{ app_name }}.{{ view_name }}`
AS
{% for channel in channels_info -%}
{% if not loop.first -%}
UNION ALL
{% endif -%}
SELECT
  "{{ channel.channel_dataset }}" AS normalized_app_id,
  "{{ channel.channel_name }}" AS normalized_channel,
  *,
FROM
  `{{ project_id }}.{{ channel.channel_dataset }}.{{ view_name }}`
{% endfor %}
