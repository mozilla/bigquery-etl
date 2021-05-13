-- Generated via ./bqetl glean_usage generate
CREATE OR REPLACE VIEW
    `{{ project_id }}.{{ target_view }}`
AS
{% for (dataset, channel) in datasets -%}
{% if not loop.first -%}
UNION ALL
{% endif -%}
{% if app_name == "fenix" -%}
SELECT e.* EXCEPT (events) REPLACE(mozfun.norm.fenix_app_info("{{ dataset }}", client_info.app_build).channel AS normalized_channel), event.*
{% else -%}
SELECT e.* EXCEPT (events) REPLACE("{{ channel }}" AS normalized_channel), event.*
{% endif -%}
FROM `{{ project_id }}.{{ dataset }}.events` e
LEFT JOIN UNNEST(e.events) AS event
{% endfor %}
