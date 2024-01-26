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
WITH daily_users_sum AS (
  SELECT
    SUM(daily_users),
  FROM
    {%- raw %}
    `{{ project_id }}.{{ dataset_id }}.{{ table_name }}` {%- endraw %}
  WHERE
    submission_date = @submission_date
    {% if app_name == "focus_android" -%}
      AND app_name IN ('Focus Android Glean', 'Focus Android Glean BrowserStack')
    {% endif -%}
),
distinct_client_count_base AS (
  {%- for channel in channels %}
    {%- if not loop.first -%}
      UNION ALL
    {%- endif %}
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
    ABS((SELECT * FROM daily_users_sum) - (SELECT * FROM distinct_client_count)) > 10,
    ERROR(
      CONCAT(
        "Daily users mismatch between the {{ app_name }} live across all channels ({%- for channel in channels %}{{ channel.table }},{% endfor -%}) and active_users_aggregates ({%- raw %}`{{ dataset_id }}.{{ table_name }}`{%- endraw %}) tables is greater than 10.",
        " Live table count: ",
        (SELECT * FROM distinct_client_count),
        " | active_users_aggregates (daily_users): ",
        (SELECT * FROM daily_users_sum),
        " | Delta detected: ",
        ABS((SELECT * FROM daily_users_sum) - (SELECT * FROM distinct_client_count))
      )
    ),
    NULL
  );
