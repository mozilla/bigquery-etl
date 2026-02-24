{{ header }}

CREATE OR REPLACE VIEW
  `{{ project_id }}.{{ target_view }}`
AS
WITH events_first_seen_union AS (
  {% for (dataset, channel) in datasets %}
    {% if not loop.first -%}
      UNION ALL BY NAME
    {% endif -%}
    SELECT
    "{{ dataset }}" AS normalized_app_id,
    * REPLACE("{{ channel }}" AS normalized_channel)
    FROM `{{ project_id }}.{{ dataset }}.{{ table }}`
  {% endfor %}
)
SELECT
  *
FROM events_first_seen_union
