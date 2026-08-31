{#
  submission_date WHERE-clause filter with three modes:

  1. bqetl-render mode (var bqetl_render=true): emit `<col> = @submission_date`, the
     bigquery-etl parameter form. Compile with this var (`dbt compile --vars
     '{"bqetl_render": true}'`) to render SQL you can copy into a bigquery-etl query.sql.
  2. explicit date (var submission_date=YYYY-MM-DD): filter that single day. Used for
     dev runs and backfills.
  3. default: yesterday (CURRENT_DATE - 1).

  When the column name ends in `_timestamp` (e.g. submission_timestamp) it is wrapped
  in DATE() so the comparison is by day.
#}
{% macro submission_date_filter(column='submission_date') %}
  {%- set lhs = 'DATE(' ~ column ~ ')' if column.endswith('_timestamp') else column -%}
  {%- if var('bqetl_render', false) -%}
  {{ lhs }} = @submission_date
  {%- elif var('submission_date', none) -%}
  {{ lhs }} = DATE '{{ var('submission_date') }}'
  {%- else -%}
  {{ lhs }} = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
  {%- endif -%}
{% endmacro %}
