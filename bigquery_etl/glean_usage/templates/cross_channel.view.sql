-- Generated via ./bqetl glean_usage generate
CREATE OR REPLACE VIEW
    `{{ project_id }}.{{ target_view }}`
AS
{% for (dataset, channel) in datasets -%}
{% if not loop.first -%}
UNION ALL
{% endif -%}
{% if app_name == "fenix" -%}
SELECT * REPLACE(mozfun.norm.fenix_app_info("{{ dataset }}", app_build_id).channel AS normalized_channel)
{% else -%}
SELECT * REPLACE("{{ channel }}" AS normalized_channel)
{% endif -%}
FROM `{{ project_id }}.{{ dataset }}.{{ table }}`
{% endfor %}
