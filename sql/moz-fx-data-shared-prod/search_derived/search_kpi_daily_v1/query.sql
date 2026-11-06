-- One row per day, dimension set and component, with client_id removed. A zero value makes
-- no row, so client_count counts clients with value > 0 for that component.
SELECT
  submission_date,
  platform,
  country,
  normalized_channel,
  os,
  app_version_major,
  normalized_default_search_engine,
  meets_population_rules,
  is_bot,
  definition_version,
  unpivoted.name AS component,
  SUM(unpivoted.value) AS value,
  COUNT(DISTINCT client_id) AS client_count
FROM
  `moz-fx-data-shared-prod.search_derived.search_kpi_clients_daily_v1`
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
  submission_date,
  platform,
  country,
  normalized_channel,
  os,
  app_version_major,
  normalized_default_search_engine,
  meets_population_rules,
  is_bot,
  definition_version,
  component
