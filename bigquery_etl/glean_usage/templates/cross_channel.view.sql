-- Generated via ./bqetl glean_usage generate
CREATE OR REPLACE VIEW
    `{{ project_id }}.{{ target_view }}`
AS
{% for (dataset, channel) in datasets -%}
{% if not loop.first -%}
UNION ALL
{% endif -%}
SELECT * REPLACE("{{ channel }}" AS normalized_channel)
FROM {{ dataset }}.{{ table }}
{% endfor %}
