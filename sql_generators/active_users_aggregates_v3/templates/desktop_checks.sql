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
{% raw -%}

#warn
WITH dau_sum AS (
  SELECT
    SUM(dau),
  FROM
    `{{ project_id }}.{{ dataset_id }}.{{ table_name }}`
  WHERE
    submission_date = @submission_date
),
live_table_dau_count_base AS (
  SELECT
    client_id,
    SUM(
      payload.processes.parent.scalars.browser_engagement_total_uri_count_normal_and_private_mode
    ) AS scalar_parent_browser_engagement_total_uri_count_normal_and_private_mode_sum,
    SUM(
      payload.processes.parent.scalars.browser_engagement_total_uri_count
    ) AS scalar_parent_browser_engagement_total_uri_count_sum,
    SUM(
      COALESCE(
        payload.processes.parent.scalars.browser_engagement_active_ticks,
        payload.simple_measurements.active_ticks
      )
    ) AS active_ticks
  FROM
    `moz-fx-data-shared-prod.telemetry_live.main_v5`
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND normalized_app_name = 'Firefox'
    AND document_id IS NOT NULL
  GROUP BY
    client_id
),
overactive AS (
  SELECT
    client_id
  FROM
    live_table_dau_count_base
  GROUP BY
    client_id
  HAVING
    COUNT(*) > 150000
),
client_summary AS (
  SELECT
    client_id,
    SUM(
      COALESCE(
        scalar_parent_browser_engagement_total_uri_count_normal_and_private_mode_sum,
        scalar_parent_browser_engagement_total_uri_count_sum
      )
    ) AS total_uri_count,
    SUM(active_ticks / (3600 / 5)) AS active_hours_sum,
  FROM
    live_table_dau_count_base
  LEFT JOIN
    overactive
    USING (client_id)
  WHERE
    overactive.client_id IS NULL
  GROUP BY
    client_id
),
last_seen AS (
  SELECT
    client_id,
    days_since_seen,
  FROM
    `moz-fx-data-shared-prod.telemetry.clients_last_seen`
  WHERE
    submission_date = @submission_date
),
live_table_dau_count AS (
  SELECT
    COUNTIF(active_hours_sum > 0 AND total_uri_count > 0 AND days_since_seen = 0)
  FROM
    client_summary
  LEFT JOIN
    last_seen
    USING (client_id)
)
SELECT
  IF(
    ABS((SELECT * FROM dau_sum) - (SELECT * FROM live_table_dau_count)) > 10,
    ERROR(
      CONCAT(
        "DAU mismatch between the live (`telemetry_live.main_v5`) and active_users_aggregates (`{{ dataset_id }}.{{ table_name }}`) tables is greater than 10.",
        " Live table count: ",
        (SELECT * FROM live_table_dau_count),
        " | active_users_aggregates (DAU): ",
        (SELECT * FROM dau_sum),
        " | Delta detected: ",
        ABS((SELECT * FROM dau_sum) - (SELECT * FROM live_table_dau_count))
      )
    ),
    NULL
  );

#warn
WITH daily_users_sum AS (
  SELECT
    SUM(daily_users),
  FROM
    `{{ project_id }}.{{ dataset_id }}.{{ table_name }}`
  WHERE
    submission_date = @submission_date
),
distinct_client_count_base AS (
  SELECT
    client_id
  FROM
    `moz-fx-data-shared-prod.telemetry_live.main_v5`
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND normalized_app_name = 'Firefox'
    AND document_id IS NOT NULL
),
overactive AS (
  SELECT
    client_id
  FROM
    distinct_client_count_base
  GROUP BY
    client_id
  HAVING
    COUNT(*) > 150000
),
distinct_client_count AS (
  SELECT
    COUNT(DISTINCT client_id),
  FROM
    distinct_client_count_base
  FULL OUTER JOIN
    overactive
    USING (client_id)
  WHERE
    overactive.client_id IS NULL
)
SELECT
  IF(
    ABS((SELECT * FROM daily_users_sum) - (SELECT * FROM distinct_client_count)) > 10,
    ERROR(
      CONCAT(
        "Daily_users mismatch between the live (`telemetry_live.main_v5`) and active_users_aggregates (`{{ dataset_id }}.{{ table_name }}`) tables is greater than 10.",
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

WITH dau_current AS (
  SELECT
    SUM(dau) AS dau
  FROM
    `{{ project_id }}.{{ dataset_id }}.{{ table_name }}`
  WHERE
    submission_date = @submission_date
),
dau_previous AS (
  SELECT
    SUM(dau) AS dau
  FROM
    `{{ project_id }}.{{ dataset_id }}.{{ table_name }}`
  WHERE
    submission_date = DATE_SUB(@submission_date, INTERVAL 1 DAY)
)
SELECT
  IF(
    ABS((SELECT SUM(dau) FROM dau_current) / (SELECT SUM(dau) FROM dau_previous)) > 1.5,
    ERROR(
      "Current date's DAU is 50% higher than in previous date. See source table (`{{ project_id }}.{{ dataset_id }}.{{ table_name }}`)!"
    ),
    NULL
  );

{% endraw %}
