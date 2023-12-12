-- todo: make a script


-- Generated via ./bqetl generate glean_usage
-- This table aggregates event flows across Glean applications.
{% for app in apps %}
{% set outer_loop = loop -%}
{% for dataset in app -%}
{% if dataset['bq_dataset_family'] not in ["telemetry", "accounts_frontend", "accounts_backend"] %}  
  {% if not outer_loop.first -%}
  UNION ALL
  {% endif -%}

{% endif %}
{% endfor %}
{% endfor %}
