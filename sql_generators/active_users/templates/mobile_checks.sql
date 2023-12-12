{#
   We use raw here b/c the first pass is rendered to create the checks.sql
   files, and the second pass is rendering of the checks themselves.
   Without/outside the {% raw %} the macros would be rendered for every
   check file when we create the checks file, when `bqetl generate active_users`
   is called.
   Inside the {% raw %} the checks get rendered when we _run_ the check,
   during `bqetl query backfill`.
   (you can also run them locally with `bqetl check run`).
#}
#warn
WITH dau_sum AS (
  SELECT
    SUM(dau),
  FROM
    {%- raw %}
    `{{ project_id }}.{{ dataset_id }}.{{ table_name }}` {% endraw -%}
  WHERE
    submission_date = @submission_date
),
distinct_client_count_base AS (
  {%- for channel in channels %}
  {%- if not loop.first -%}
  UNION ALL
    {%- endif %}
    {% if channel.name -%}
  -- {{ channel.name }} channel
    {% endif -%}
  SELECT
    COUNT(DISTINCT client_info.client_id) AS distinct_client_count,
  FROM
    {{ channel.table }}

  WHERE
    DATE(submission_timestamp) = @submission_date
{% endfor -%}
),
distinct_client_count AS (
  SELECT
    SUM(distinct_client_count)
  FROM
    distinct_client_count_base
)
SELECT
  IF(
    (SELECT * FROM dau_sum) <> (SELECT * FROM distinct_client_count),
    ERROR("DAU mismatch between aggregates table and live table"),
    NULL
  );
