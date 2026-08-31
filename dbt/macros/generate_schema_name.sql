{#
  Derive the BigQuery dataset from the model's folder, mirroring bigquery-etl's
  `<dataset>/<table>` layout: a model at models/<dataset>/<model>.sql lands in a dataset
  named <dataset>. No per-folder config in dbt_project.yml is needed -- just drop a model
  into a folder named after the dataset you want.

  Resolution order:
    1. An explicit `+schema` (custom_schema_name) always wins, if set.
    2. Otherwise use the model's immediate parent folder (node.fqn[-2]). fqn looks like
       ['dbt_dev', 'firefox_desktop_derived', 'baseline_active_users_aggregates_v2'], so
       fqn[-2] is the containing folder.
    3. Models directly under models/ (no subfolder) and other nodes fall back to the
       profile's `dataset` (target.schema).

  Note: unlike default dbt, this does NOT prefix target.schema, so dataset names stay
  clean (firefox_desktop_derived, not dev_firefox_desktop_derived).
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
  {%- if custom_schema_name is not none -%}
    {{ custom_schema_name | trim }}
  {%- elif node is not none and node.fqn | length > 2 -%}
    {{ node.fqn[-2] }}
  {%- else -%}
    {{ target.schema }}
  {%- endif -%}
{%- endmacro %}
