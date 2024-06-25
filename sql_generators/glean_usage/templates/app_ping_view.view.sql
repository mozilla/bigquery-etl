-- Generated via ./bqetl generate glean_usage
CREATE OR REPLACE VIEW
  `{{ target_view }}`
AS
{% for query in queries -%}
{% if not loop.first -%}
UNION ALL
{% endif -%}
SELECT
  "{{ query.dataset }}" AS normalized_app_id,
  {% if query.channel and query.app_name != "fenix" -%}
  "{{ query.channel }}" AS normalized_channel,
  {% elif query.app_name == "fenix" and query.includes_client_info %}
  mozfun.norm.fenix_app_info("{{ query.dataset }}", client_info.app_build).channel AS normalized_channel,
  {% elif query.app_name == "fenix" and not query.includes_client_info %}
    -- set app build to 21850000 since all affected pings are coming from versions older than that
    -- abb build only used to differentiate between preview (pre 21850000) and nightly (everything after)
    mozfun.norm.fenix_app_info("{{ query.dataset }}", '21850000').channel AS normalized_channel,
  {% else %}
  normalized_channel,
  {% endif %}
  {{ query.select_expression }}
FROM `{{ project_id }}.{{ query.dataset }}.{{ query.table }}`
{% endfor %}
