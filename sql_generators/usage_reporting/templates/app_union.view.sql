-- {{ header }}
CREATE OR REPLACE VIEW
  `{{ project_id }}.{{ app_name }}.{{ view_name }}`
AS
{% for channel in channels_info -%}
{% if not loop.first -%}
UNION ALL
{% endif -%}
SELECT
  {% if app_name == "firefox_desktop" %}
  "{{ channel_dataset }}" AS normalized_app_id,
  `mozfun.norm.app_channel`(app_channel) AS normalized_channel,
  {% else %}
  "{{ channel.channel_dataset }}" AS normalized_app_id,
  "{{ channel.channel_name }}" AS normalized_channel,
  {% endif %}
  *,
FROM
  {% if app_name == "firefox_desktop" %}
  `{{ project_id }}.{{ channel.channel_dataset }}_derived.{{ view_name }}_v1`
  {% else %}
  `{{ project_id }}.{{ channel.channel_dataset }}.{{ view_name }}`
  {% endif %}
{% endfor %}
