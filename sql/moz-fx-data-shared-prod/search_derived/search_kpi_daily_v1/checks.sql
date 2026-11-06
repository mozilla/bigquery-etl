#fail
{{ min_row_count(1, "submission_date = @submission_date") }}

#fail
-- Every component must sum exactly to the client table, on every platform.
WITH daily AS (
  SELECT
    platform,
    component,
    SUM(value) AS value
  FROM
    `{{ project_id }}.{{ dataset_id }}.{{ table_name }}`
  WHERE
    submission_date = @submission_date
  GROUP BY
    platform,
    component
),
clients AS (
  SELECT
    platform,
    unpivoted.name AS component,
    SUM(unpivoted.value) AS value
  FROM
    `{{ project_id }}.{{ dataset_id }}.search_kpi_clients_daily_v1`
  CROSS JOIN
    UNNEST(
      [
        STRUCT('sap' AS name, sap AS value),
        STRUCT('sap_excluded' AS name, sap_excluded AS value),
        STRUCT('suggest_sponsored' AS name, suggest_sponsored AS value),
        STRUCT('suggest_nonsponsored' AS name, suggest_nonsponsored AS value),
        STRUCT('ai_chat' AS name, ai_chat AS value)
      ]
    ) AS unpivoted
  WHERE
    submission_date = @submission_date
    AND unpivoted.value > 0
  GROUP BY
    platform,
    component
),
differences AS (
  SELECT
    FORMAT(
      "%s %s: %d here, %d in search_kpi_clients_daily_v1",
      platform,
      component,
      daily.value,
      clients.value
    ) AS difference
  FROM
    daily
  FULL JOIN
    clients
    USING (platform, component)
  WHERE
    daily.value IS DISTINCT FROM clients.value
)
SELECT
  IF(COUNT(*) > 0, ERROR(STRING_AGG(difference, "; ")), NULL)
FROM
  differences;

#fail
-- A day is built with one definition, so its components can be compared.
SELECT
  IF(
    COUNT(DISTINCT definition_version) > 1,
    ERROR("More than one definition_version in this partition"),
    NULL
  )
FROM
  `{{ project_id }}.{{ dataset_id }}.{{ table_name }}`
WHERE
  submission_date = @submission_date;
