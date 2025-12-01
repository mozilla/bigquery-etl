{% from 'macros.sql' import event_extras_by_type_struct -%}
-- Generated via ./bqetl generate glean_usage
CREATE OR REPLACE VIEW
  `{{ project_id }}.{{ target_view }}`
AS
WITH events_stream_union AS (
  {% for (dataset, channel) in datasets %}
    {% if not loop.first -%}
      UNION ALL BY NAME
    {% endif -%}
    SELECT
      "{{ dataset }}" AS normalized_app_id,
      e.*
      {% if app_name == "fenix" -%}
        REPLACE(mozfun.norm.fenix_app_info("{{ dataset }}", client_info.app_build).channel AS normalized_channel),
      {% elif datasets|length > 1 -%}
        REPLACE("{{ channel }}" AS normalized_channel),
      {% endif -%}
    FROM `{{ project_id }}.{{ dataset }}_derived.events_stream_v1` AS e
  {% endfor %}
)
SELECT
  *,
  {% if extras_by_type %}
    {{ event_extras_by_type_struct(extras_by_type) }} AS extras
  {% endif %}
FROM events_stream_union
